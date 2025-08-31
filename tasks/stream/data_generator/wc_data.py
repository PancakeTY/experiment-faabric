#!/usr/bin/env python3
import urllib.request
import os
import sys

# ——————— CONFIG ———————
# List your Gutenberg IDs here, e.g. [1342, 11, 84, …]
# Or alternatively uncomment the "read from file" section below.

# fmt: off
book_ids = [
    1342, 11, 84, 1661, 2701, 345, 98, 140, 74, 76,
    768, 1260, 174, 219, 2554, 2600, 1399, 4300, 28054, 161,
    158, 145, 33, 5200, 844, 35, 36, 215, 2814, 1952,
    41, 55, 1184, 135, 236, 132, 16643, 1322, 4086, 147,
    1404, 2009, 1998, 1232, 150, 46, 131, 829, 996, 1497,
    120, 16, 43, 271, 64317, 2852, 17396, 1200, 1257, 613,
    1727, 244, 105, 121, 141, 415, 20, 16328, 8800, 205,
    1526, 22120, 2680, 3207, 2130, 45180, 23, 1080, 12, 2434,
    969, 846, 289, 340, 14838, 2377, 521, 86, 1664, 10636,
    45413, 209, 580, 788, 2283, 11339, 2781, 514, 2821, 831 
]
# fmt: on

# If you'd rather keep your IDs in a text file (one per line),
# uncomment these lines and comment out the hard‐coded list above:
#
# ids_file = 'book_ids.txt'
# with open(ids_file, 'r', encoding='utf-8') as f:
#     book_ids = [int(line.strip()) for line in f if line.strip().isdigit()]

# Output path (will be created if necessary)
output_path = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/wc_dataset.txt"
)
# ———————————————————


def fetch_book_text(book_id):
    url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[ERROR] could not fetch ID {book_id}: {e}", file=sys.stderr)
        return None


def main():
    # ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as out_f:
        for book_id in book_ids:
            print(f"Fetching book {book_id}…")
            text = fetch_book_text(book_id)
            if not text:
                print(f"  → skipped {book_id}", file=sys.stderr)
                continue

            # Optionally, write a simple separator or header:
            out_f.write(f"\n\n===== Start of book {book_id} =====\n\n")
            out_f.write(text)
            out_f.write(f"\n\n===== End of book {book_id} =====\n")

    print(f"\nDone. Wrote {len(book_ids)} books to\n  {output_path}")


if __name__ == "__main__":
    main()
