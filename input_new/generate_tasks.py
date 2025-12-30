import json
import random
import sys

sizes = ["200x200", "400x400", "800x800"]


def main(n: int, out_path: str):
    data = []
    for i in range(n):
        key = f"img/{i:06d}.jpg"
        size = random.choice(sizes)
        data.append({"image_key": key, "size": size})
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    out = sys.argv[2] if len(sys.argv) > 2 else "sample_tasks_gen.json"
    main(n, out)
