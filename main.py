import argparse
from jobs.bronze_layer import ingest_daily_orders
def main():
    parser = argparse.ArgumentParser(description='Get input file path')
    parser.add_argument('input_file', type=str, help='Path to the input file')
    args = parser.parse_args()

    input_file_path = args.input_file
    print("Input file path:", input_file_path)
    ingest_daily_orders.run(input_file_path)


if __name__ == "__main__":
    main()



