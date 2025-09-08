from nebulagraph.client import NebulagraphTPCH


def main():
    client = NebulagraphTPCH()
    client.connect()
    client.clear()
    client.setup()
    client.load()
    client.close()


if __name__ == "__main__":
    main()
