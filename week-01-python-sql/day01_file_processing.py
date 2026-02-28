import csv
import math
import logging
from tqdm import tqdm
import os
# ─────────────────────────────────────────
# Logger configuration (only once, outside the function)
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process.log'),
        logging.StreamHandler()
    ]
)


def split_csv_in_chunks(
    file_route: str,
    chunk_size: int = 10000,
    output_prefix: str = "new_file",
    encoding: str = 'utf-8'
) -> int:
    """
    Splits a large CSV file into multiple smaller files.

    Args:
        file_route    : Path to the original CSV file.
        chunk_size    : Number of rows per output file.
        output_prefix : Prefix used to name the generated files.
        encoding      : Encoding of the CSV file.

    Returns:
        file_counter  : Total number of files created.
    """
    header_df = []
    whole_data = []
    file_counter = 0

    try:
        # Count rows without loading the entire file into memory
        row_count = sum(1 for _ in open(file_route, encoding=encoding)) - 1
        qty_files = math.ceil(row_count / chunk_size)
        logging.info(f"File: {file_route}")
        logging.info(f"Total rows: {row_count:,} | Chunk size: {chunk_size:,} | Expected files: {qty_files}")

        with open(file_route, 'r', newline="", encoding=encoding) as huge_data:
            file_read = csv.reader(huge_data)
            header_df.append(next(file_read))# next() read only first row
            #so the next iterator will be start from the first row not header
            for id_element, data in tqdm(enumerate(file_read,start = 1), total=row_count, desc="Processing CSV"):

                # Accumulate rows into current chunk
                whole_data.append(data)

                # When chunk is full, write output file
                if len(whole_data) == chunk_size:
                    file_counter += 1
                    output_name = f'{output_prefix}_{file_counter}.csv'
                    with open(output_name, 'w', newline="", encoding=encoding) as output_file:
                        writer = csv.writer(output_file)
                        writer.writerows(header_df)
                        writer.writerows(whole_data)
                    logging.info(f"Created: {output_name} | Rows: {chunk_size:,}")

                    whole_data.clear()

        # Write last chunk if remaining rows exist
        if len(whole_data) > 0:
            file_counter += 1
            output_name = f'{output_prefix}_{file_counter}.csv'
            with open(output_name, 'w', newline="", encoding=encoding) as output_file:
                writer = csv.writer(output_file)
                writer.writerows(header_df)
                writer.writerows(whole_data)
            logging.info(f"Created (last): {output_name} | Rows: {len(whole_data):,}")

    except FileNotFoundError:
        logging.error(f"File not found: {file_route}")
        raise
    except PermissionError:
        logging.error(f"Permission denied: {file_route}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

    logging.info(f"Process completed | Total files created: {file_counter}")
    return file_counter


# ─────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    file_route = r'C:/Users/HP/Downloads/gaming_mental_health_10M_40features.csv'

    total = split_csv_in_chunks(
        file_route=file_route,
        chunk_size=100000,
        output_prefix="gaming_mental_health"
    )

    print(f"\nTotal files generated: {total}")








