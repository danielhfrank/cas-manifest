import argparse

from hashfs import HashFS

from .registry import Registry


def main(cas_dir, hash_str):
    fs = HashFS(cas_dir, depth=1, width=2)
    r = Registry.dataset(fs)
    cd = r.load(hash_str)
    loaded = cd.load_from(fs)
    print(loaded[0])
    cd.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cas-dir', help='root of local cas directory', required=True)
    parser.add_argument('--hash', help='Hash of dataset to validate')
    args = parser.parse_args()

    main(args.cas_dir, args.hash)
