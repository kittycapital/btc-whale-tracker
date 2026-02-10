"""
BTC Whale Tracker - Bitinfocharts Rich List Scraper
Scrapes top BTC addresses from bitinfocharts.com (free, no API key)
+ CoinGecko for BTC price
Runs daily via GitHub Actions at 7AM KST
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError


# ============================================================
# KNOWN CEX & CUSTODIAL ADDRESSES TO EXCLUDE
# ============================================================
CEX_ADDRESSES = {
    # Binance
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s",
    "3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb",
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j",
    "39884E3j6KZj82FK4hA3t6K5UMRSSamHdC",
    "3HcEUoSEQEbNjLnHwvLLibajtFShhBJi3M",
    "bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9",
    "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfl6t6c",
    "bc1ql49ydapnjafl5t2cp9zqpjwe6pdgmxy98859v2",
    # Bitfinex
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g",
    # Coinbase
    "3FHNBLobJnbCTFTVakh5TXmEneyf5PT61B",
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS",
    "3FM6FypcrSVhdHh7KqBDvKTiXFCVRVHBZh",
    "bc1qjh0akslml59uuczddqu0y4p3vj64hg5kxzwf9k",
    "bc1q7cyrfmck2ffu2ud3rn5l5a8yv6f0chkp0zpemf",
    "395xkFtQVeos4qiCkhNphAg4CDHb8TpEfm",
    "3CySiMbeSkhPbcZNXbXYrAiGKmoney3aSU",
    # Kraken
    "3AfWk15VsMKp8VJnBt3Qpd9ayqiMBpijh1",
    "3FupZp77ySr7jwoLYEJ9mwzJpvoNBXsBnE",
    "bc1qxfhwwh6z47x3g08k5jnc27k3nx5q4k8cqycat0",
    # Gemini
    "36NkTqCAApfRJBKicQaqrdKs29g6hyE4LSP",
    "3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r",
    # Bitstamp
    "3P3QsMVK89JBNqZQv5zMAKG8FK3kJM4rjt",
    "3BitnP5v17WpVKSUoseFqEjGQCETv6rkRk",
    # OKX
    "1FzWLkAahHooV3kzTgyx6qsXoRDrBYBMU4",
    "1Lhurpe3VYtfWZmVFLTwGjz7u3dSCnYiir",
    # Huobi / HTX
    "1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ",
    "14u4nA5sugaSwb6SZgn5av2vuChdMnD9E5",
    # Bybit
    "bc1qlh83hwfx3e2fpuqjnmhm7y4h54ehtggmfvalh3",
    # Crypto.com
    "3LQUu4v9z6KNch71j7kbj8GPeAGUo1FW6a",
    "3Gk6bRDHLi6c7UHqePYRBCkPSweQ3LhURU",
    # Robinhood
    "3EmUH8Uh9EXE7axgyAeBsCc2vdUdKkDqWK",
    "bc1qr35hws365juz5rtlsjtvmlu578guf5zy5kf5v3",
    # Mt.Gox Trustee
    "17A16QmavnUfCW11DAApiJxp7ARnxN5pGX",
    "1AsHPP7WcGRsBkYLpSv7HAEjFnBBjPFkv1",
    "1HeHLv7ZRFxWUVjuWkWT2gECuLYBs2HPKD",
    # US Government Seized
    "bc1qa5wkgaew2dkv56kc6hp24cc2nidak9namkqree",
    # Grayscale GBTC
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64",
    "3LQQTBh992TcnW3Pi3mn42tAF3cQqLp5YJ",
    # Block.one
    "3BtZ3VN4GTPieFHDyqAJjhNpYqxa5qDiAA",
    # Tether Treasury
    "1NTMakcgVwQpMdGxRQnFKCNDZQFRkqqvJ1",
    # Upbit (Mr. 100)
    "3FaBxEFBpSLCzFGCPQFyQEfwGMRyjoZGAT",
}

# Known CEX labels from Bitinfocharts (partial match in label text)
CEX_LABELS = [
    "binance", "coinbase", "bitfinex", "kraken", "gemini",
    "bitstamp", "okex", "okx", "huobi", "htx", "bybit",
    "crypto.com", "robinhood", "upbit", "bithumb", "bitflyer",
    "kucoin", "gate.io", "mexc", "bitget", "deribit",
    "bittrex", "poloniex", "luno", "blockchain.com",
]


def http_get(url, retries=3):
    """HTTP GET with retries and User-Agent."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    req = Request(url, headers=headers)

    for attempt in range(retries):
        try:
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError) as e:
            print(f"  HTTP error on attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
        except Exception as e:
            print(f"  Error on attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


def fetch_btc_price():
    """Get BTC price from CoinGecko (free, no key)."""
    print("Fetching BTC price from CoinGecko...")
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    try:
        req = Request(url, headers={"User-Agent": "BTC-Whale-Tracker/1.0"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            price = data.get("bitcoin", {}).get("usd", 0)
            print(f"  BTC price: ${price:,.2f}")
            return price
    except Exception as e:
        print(f"  CoinGecko error: {e}")
    return 0


def parse_rich_list_page(html_content):
    """Parse Bitinfocharts rich list HTML to extract address data."""
    addresses = []

    # Find table rows
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_content, re.DOTALL)

    for row in rows:
        # Extract address from link to /bitcoin/address/
        addr_match = re.search(
            r'/bitcoin/address/([13bc][a-zA-Z0-9]{25,62})',
            row
        )
        if not addr_match:
            continue

        address = addr_match.group(1).strip()

        # Extract all cell contents
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        # Extract balance - look for BTC amount
        balance_btc = 0.0
        for cell in cells:
            btc_match = re.search(r'([\d,]+\.?\d*)\s*BTC', cell)
            if btc_match:
                try:
                    balance_btc = float(btc_match.group(1).replace(",", ""))
                    break
                except ValueError:
                    continue

        if balance_btc <= 0:
            continue

        # Extract label/owner tag
        label = ""
        # Check for wallet: tag or small text label
        label_match = re.search(r'wallet:\s*([^<"]+)', row)
        if label_match:
            label = label_match.group(1).strip()
        else:
            label_match = re.search(r'<small[^>]*>\s*(?:<a[^>]*>)?\s*([^<]+)', row)
            if label_match:
                lbl = label_match.group(1).strip()
                if lbl and len(lbl) > 1 and lbl not in ("...", "address"):
                    label = lbl

        # Extract ins/outs count from cells
        tx_count = 0
        for cell in cells:
            # Look for plain numbers that could be tx counts
            num_match = re.search(r'^\s*([\d,]+)\s*$', cell.strip())
            if num_match:
                try:
                    val = int(num_match.group(1).replace(",", ""))
                    if val > tx_count:
                        tx_count = val
                except ValueError:
                    pass

        addresses.append({
            "address": address,
            "balance_btc": balance_btc,
            "label": label,
            "tx_count": tx_count,
        })

    return addresses


def scrape_rich_list():
    """Scrape top 200 BTC addresses from Bitinfocharts."""
    all_addresses = []

    # Page 1: top 1-100
    print("Scraping Bitinfocharts page 1 (top 1-100)...")
    html1 = http_get("https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html")
    if html1:
        addrs1 = parse_rich_list_page(html1)
        print(f"  Parsed {len(addrs1)} addresses from page 1")
        all_addresses.extend(addrs1)
    else:
        print("  ERROR: Failed to fetch page 1")
        return []

    time.sleep(3)

    # Page 2: top 101-200
    print("Scraping Bitinfocharts page 2 (top 101-200)...")
    html2 = http_get("https://bitinfocharts.com/top-100-richest-bitcoin-addresses-2.html")
    if html2:
        addrs2 = parse_rich_list_page(html2)
        print(f"  Parsed {len(addrs2)} addresses from page 2")
        all_addresses.extend(addrs2)
    else:
        print("  WARNING: Failed to fetch page 2, continuing with page 1 only")

    return all_addresses


def is_cex_address(address, label):
    """Check if address belongs to a known CEX."""
    if address in CEX_ADDRESSES:
        return True
    if label:
        label_lower = label.lower()
        for cex in CEX_LABELS:
            if cex in label_lower:
                return True
    return False


def classify_tier(balance_usd):
    """Classify whale tier by USD value."""
    if balance_usd >= 100_000_000:
        return "$100M+"
    elif balance_usd >= 50_000_000:
        return "$50M-100M"
    elif balance_usd >= 10_000_000:
        return "$10M-50M"
    else:
        return "<$10M"


def load_previous_snapshot(filepath):
    """Load previous snapshot."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def calculate_changes(current_whales, snapshots_dir):
    """Calculate balance changes from historical snapshots."""
    changes = {}
    now = datetime.now(timezone.utc)

    for period, days in [("1d", 1), ("7d", 7), ("30d", 30)]:
        target_files = []
        if os.path.exists(snapshots_dir):
            for f in os.listdir(snapshots_dir):
                if f.startswith("snapshot_") and f.endswith(".json"):
                    try:
                        date_str = f.replace("snapshot_", "").replace(".json", "")
                        file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        diff = abs((now - file_date).days - days)
                        if diff <= 1:
                            target_files.append((diff, f))
                    except ValueError:
                        continue

        if target_files:
            target_files.sort()
            best_file = target_files[0][1]
            snapshot = load_previous_snapshot(os.path.join(snapshots_dir, best_file))
            if snapshot:
                old_balances = {w["address"]: w["balance_btc"] for w in snapshot.get("whales", [])}
                for whale in current_whales:
                    addr = whale["address"]
                    if addr not in changes:
                        changes[addr] = {}
                    old_bal = old_balances.get(addr)
                    if old_bal is not None:
                        changes[addr][period] = round(whale["balance_btc"] - old_bal, 8)
                    else:
                        changes[addr][period] = None

    return changes


def main():
    print("=" * 60)
    print("BTC Whale Tracker - Bitinfocharts Scraper")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    data_dir = os.environ.get("DATA_DIR", "data")
    snapshots_dir = os.path.join(data_dir, "snapshots")
    os.makedirs(snapshots_dir, exist_ok=True)

    # Step 1: BTC price
    btc_price = fetch_btc_price()
    if btc_price == 0:
        print("ERROR: Could not fetch BTC price. Aborting.")
        sys.exit(1)

    # Step 2: Scrape rich list
    raw_addresses = scrape_rich_list()
    if not raw_addresses:
        print("ERROR: Could not scrape rich list. Aborting.")
        sys.exit(1)

    print(f"\nTotal addresses scraped: {len(raw_addresses)}")

    # Step 3: Filter CEX
    filtered = []
    cex_removed = 0
    for addr in raw_addresses:
        if is_cex_address(addr["address"], addr.get("label", "")):
            cex_removed += 1
            print(f"  Excluded CEX: {addr['address'][:24]}... [{addr.get('label', '')}] ({addr['balance_btc']:,.2f} BTC)")
        else:
            filtered.append(addr)

    print(f"  Filtered: {cex_removed} CEX removed, {len(filtered)} remaining")

    # Step 4: Build whale list
    whales = []
    for addr in filtered:
        balance_usd = addr["balance_btc"] * btc_price
        if balance_usd >= 10_000_000:
            whales.append({
                "address": addr["address"],
                "balance_btc": round(addr["balance_btc"], 8),
                "balance_usd": round(balance_usd, 2),
                "tier": classify_tier(balance_usd),
                "tx_count": addr.get("tx_count", 0),
                "label": addr.get("label", ""),
                "change_1d": None,
                "change_7d": None,
                "change_30d": None,
            })

    whales.sort(key=lambda x: x["balance_usd"], reverse=True)
    top_whales = whales[:100]
    print(f"\nFound {len(top_whales)} whales above $10M threshold")

    # Step 5: Balance changes
    changes = calculate_changes(top_whales, snapshots_dir)
    for whale in top_whales:
        addr = whale["address"]
        whale["change_1d"] = changes.get(addr, {}).get("1d")
        whale["change_7d"] = changes.get(addr, {}).get("7d")
        whale["change_30d"] = changes.get(addr, {}).get("30d")

    # Step 6: Tier summary
    tier_counts = {}
    tier_totals = {}
    for whale in top_whales:
        t = whale["tier"]
        tier_counts[t] = tier_counts.get(t, 0) + 1
        tier_totals[t] = tier_totals.get(t, 0) + whale["balance_btc"]

    tiers = []
    for tier_name in ["$100M+", "$50M-100M", "$10M-50M"]:
        tiers.append({
            "name": tier_name,
            "count": tier_counts.get(tier_name, 0),
            "total_btc": round(tier_totals.get(tier_name, 0), 2),
            "total_usd": round(tier_totals.get(tier_name, 0) * btc_price, 2),
        })

    # Step 7: Sentiment
    net_change_btc = 0
    counted = 0
    for whale in top_whales:
        if whale.get("change_1d") is not None:
            net_change_btc += whale["change_1d"]
            counted += 1

    sentiment = "accumulating" if net_change_btc > 0 else "distributing" if net_change_btc < 0 else "neutral"

    # Step 8: Output JSON
    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "btc_price": btc_price,
        "total_whales": len(top_whales),
        "tiers": tiers,
        "sentiment": {
            "direction": sentiment,
            "net_change_btc_1d": round(net_change_btc, 4),
            "net_change_usd_1d": round(net_change_btc * btc_price, 2),
            "whales_tracked": counted,
        },
        "whales": top_whales,
        "large_transfers": [],
        "cex_excluded": cex_removed,
        "source": "bitinfocharts.com",
    }

    output_path = os.path.join(data_dir, "whales.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved whale data to {output_path}")

    # Save snapshot
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snapshot_path = os.path.join(snapshots_dir, f"snapshot_{today}.json")
    snapshot = {
        "date": today,
        "btc_price": btc_price,
        "whales": [{"address": w["address"], "balance_btc": w["balance_btc"]} for w in top_whales],
    }
    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f)
    print(f"Saved snapshot to {snapshot_path}")

    # Cleanup old snapshots (60 days)
    if os.path.exists(snapshots_dir):
        for fname in os.listdir(snapshots_dir):
            if fname.startswith("snapshot_") and fname.endswith(".json"):
                try:
                    date_str = fname.replace("snapshot_", "").replace(".json", "")
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - file_date).days > 60:
                        os.remove(os.path.join(snapshots_dir, fname))
                except ValueError:
                    continue

    print("\n✅ Done!")
    print(f"  Whales: {len(top_whales)}")
    print(f"  CEX filtered: {cex_removed}")
    print(f"  Tiers: {json.dumps(tier_counts)}")


if __name__ == "__main__":
    main()
