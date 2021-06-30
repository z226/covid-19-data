from cowidev.variants.etl import VariantsETL


def main():
    etl = VariantsETL()
    data = etl.extract()
    df = etl.transform(data)
    etl.load(df)


if __name__ == "__main__":
    main()
