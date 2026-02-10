"""
BTC Whale Tracker - Blockchair API Fetcher
Fetches top BTC addresses, filters out known CEX wallets,
and generates JSON data for the dashboard.
Free tier: 1,440 calls/day (no API key needed)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ============================================================
# KNOWN CEX & CUSTODIAL ADDRESSES TO EXCLUDE
# Sources: public documentation, blockchain explorers, Arkham
# Update this list periodically
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
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j",
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
    # OKX (OKEx)
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
    # Mt.Gox / Trustee
    "17A16QmavnUfCW11DAApiJxp7ARnxN5pGX",
    "1AsHPP7WcGRsBkYLpSv7HAEjFnBBjPFkv1",
    "1HeHLv7ZRFxWUVjuWkWT2gECuLYBs2HPKD",
    # US Government / Seized
    "bc1qa5wkgaew2dkv56kc6hp24cc2nidak9namkqree",
    # Grayscale GBTC
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64",
    "3LQQTBh992TcnW3Pi3mn42tAF3cQqLp5YJ",
    # Block.one
    "3BtZ3VN4GTPieFHDyqAJjhNpYqxa5qDiAA",
    # Tether Treasury
    "1NTMakcgVwQpMdGxRQnFKCNDZQFRkqqvJ1",
    # MicroStrategy (not CEX but institutional - keep or remove based on preference)
    # Uncomment below to exclude MicroStrategy
    # "bc1qazcm763858nkj2dz7g20jud8kczq7ycx9fue3q",
    # "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ",
}

BLOCKCHAIR_BASE = "https://api.blockchair.com"


def api_call(endpoint, params=None):
    """Make a Blockchair API call with basic error handling and rate limiting."""
    url = f"{BLOCKCHAIR_BASE}{endpoint}"
    if params:
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{param_str}"

    print(f"  API call: {url}")
    req = Request(url, headers={"User-Agent": "BTC-Whale-Tracker/1.0"})

    for attempt in range(3):
        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if data.get("context", {}).get("code") == 200:
                    return data
                else:
                    print(f"  API error code: {data.get('context', {}).get('code')}")
                    return None
        except HTTPError as e:
            if e.code == 402:
                print("  Rate limit hit. Waiting 60s...")
                time.sleep(60)
            elif e.code == 429:
                print("  IP temporarily blocked. Waiting 120s...")
                time.sleep(120)
            else:
                print(f"  HTTP Error {e.code}: {e.reason}")
                if attempt < 2:
                    time.sleep(5)
        except URLError as e:
            print(f"  URL Error: {e.reason}")
            if attempt < 2:
                time.sleep(5)
        except Exception as e:
            print(f"  Error: {e}")
            if attempt < 2:
                time.sleep(5)

    return None


def fetch_btc_price():
    """Get current BTC price from Blockchair stats."""
    print("Fetching BTC price...")
    data = api_call("/bitcoin/stats")
    if data and "data" in data:
        price = data["data"].get("market_price_usd", 0)
        print(f"  BTC price: ${price:,.2f}")
        return price
    return 0


def fetch_top_addresses(limit=200):
    """Fetch top BTC addresses by balance."""
    print(f"Fetching top {limit} BTC addresses...")
    data = api_call("/bitcoin/addresses", {
        "s": "balance(desc)",
        "limit": str(limit),
    })

    if data and "data" in data:
        addresses = data["data"]
        print(f"  Retrieved {len(addresses)} addresses")
        return addresses
    return []


def fetch_address_details(address):
    """Fetch detailed info for a specific address (recent transactions)."""
    print(f"  Fetching details for {address[:16]}...")
    data = api_call(f"/bitcoin/dashboards/address/{address}", {
        "limit": "5",
        "transaction_details": "true",
    })
    time.sleep(2.5)  # Rate limit: stay well under 30/min

    if data and "data" in data:
        addr_data = data["data"].get(address, {})
        return addr_data
    return None


def filter_cex_addresses(addresses):
    """Remove known CEX/custodial addresses."""
    filtered = []
    removed = 0
    for addr in addresses:
        if addr.get("address") not in CEX_ADDRESSES:
            filtered.append(addr)
        else:
            removed += 1
            print(f"  Excluded CEX: {addr['address'][:20]}... ({addr.get('balance', 0) / 1e8:.2f} BTC)")
    print(f"  Filtered: {removed} CEX addresses removed, {len(filtered)} remaining")
    return filtered


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
    """Load previous snapshot for balance change calculation."""
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

    # Find snapshots for 1d, 7d, 30d ago
    for period, days in [("1d", 1), ("7d", 7), ("30d", 30)]:
        # Look for snapshot closest to target date
        target_files = []
        for f in os.listdir(snapshots_dir) if os.path.exists(snapshots_dir) else []:
            if f.startswith("snapshot_") and f.endswith(".json"):
                try:
                    date_str = f.replace("snapshot_", "").replace(".json", "")
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    diff = abs((now - file_date).days - days)
                    if diff <= 1:  # Within 1 day tolerance
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
                        changes[addr][period] = whale["balance_btc"] - old_bal
                    else:
                        changes[addr][period] = None  # New whale, no prior data

    return changes


def main():
    print("=" * 60)
    print("BTC Whale Tracker - Blockchair Fetcher")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Directories
    data_dir = os.environ.get("DATA_DIR", "data")
    snapshots_dir = os.path.join(data_dir, "snapshots")
    os.makedirs(snapshots_dir, exist_ok=True)

    # Step 1: Get BTC price
    btc_price = fetch_btc_price()
    if btc_price == 0:
        print("ERROR: Could not fetch BTC price. Aborting.")
        sys.exit(1)

    # Step 2: Fetch top addresses
    raw_addresses = fetch_top_addresses(200)
    if not raw_addresses:
        print("ERROR: Could not fetch addresses. Aborting.")
        sys.exit(1)

    # Step 3: Filter CEX addresses
    filtered = filter_cex_addresses(raw_addresses)

    # Step 4: Build whale list with USD values
    whales = []
    for addr in filtered:
        balance_btc = addr.get("balance", 0) / 1e8  # satoshi to BTC
        balance_usd = balance_btc * btc_price
        if balance_usd >= 10_000_000:  # $10M threshold
            whales.append({
                "address": addr.get("address", ""),
                "balance_btc": round(balance_btc, 8),
                "balance_usd": round(balance_usd, 2),
                "tier": classify_tier(balance_usd),
                "tx_count": addr.get("transaction_count", 0),
                "first_seen": addr.get("first_seen_receiving", ""),
                "last_seen": addr.get("last_seen_receiving", ""),
            })

    whales.sort(key=lambda x: x["balance_usd"], reverse=True)
    top_whales = whales[:100]  # Top 100

    print(f"\nFound {len(top_whales)} whales above $10M threshold")

    # Step 5: Calculate balance changes from snapshots
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

    # Step 7: Fetch transaction details for top 20 whales
    print("\nFetching recent transactions for top 20 whales...")
    large_transfers = []
    for whale in top_whales[:20]:
        details = fetch_address_details(whale["address"])
        if details and "transactions" in details:
            for tx in details.get("transactions", [])[:3]:
                if isinstance(tx, dict):
                    value_btc = tx.get("balance_change", 0) / 1e8
                    value_usd = abs(value_btc) * btc_price
                    if value_usd >= 1_000_000:  # Only transfers > $1M
                        large_transfers.append({
                            "address": whale["address"],
                            "tx_hash": tx.get("hash", ""),
                            "value_btc": round(value_btc, 8),
                            "value_usd": round(value_usd, 2),
                            "direction": "in" if value_btc > 0 else "out",
                            "time": tx.get("time", ""),
                        })

    large_transfers.sort(key=lambda x: abs(x["value_usd"]), reverse=True)
    large_transfers = large_transfers[:30]  # Top 30 transfers

    # Step 8: Accumulation/Distribution metric
    net_change_btc = 0
    counted = 0
    for whale in top_whales:
        if whale.get("change_1d") is not None:
            net_change_btc += whale["change_1d"]
            counted += 1

    sentiment = "accumulating" if net_change_btc > 0 else "distributing" if net_change_btc < 0 else "neutral"

    # Step 9: Build output JSON
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
        "large_transfers": large_transfers,
        "cex_excluded": len(CEX_ADDRESSES),
    }

    # Save main data file
    output_path = os.path.join(data_dir, "whales.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved whale data to {output_path}")

    # Save daily snapshot for historical tracking
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

    # Cleanup old snapshots (keep 60 days)
    if os.path.exists(snapshots_dir):
        for fname in os.listdir(snapshots_dir):
            if fname.startswith("snapshot_") and fname.endswith(".json"):
                try:
                    date_str = fname.replace("snapshot_", "").replace(".json", "")
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - file_date).days > 60:
                        os.remove(os.path.join(snapshots_dir, fname))
                        print(f"  Cleaned up old snapshot: {fname}")
                except ValueError:
                    continue

    print("\n✅ Done!")
    print(f"  Whales: {len(top_whales)}")
    print(f"  Large transfers: {len(large_transfers)}")
    print(f"  Tiers: {json.dumps(tier_counts)}")


if __name__ == "__main__":
    main()
