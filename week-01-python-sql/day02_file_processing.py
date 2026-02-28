import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gaming_report.log'),
        logging.StreamHandler()
    ]
)


def split_csv_in_chunks(file_route: str, chunk_size: int = 10000
                        , output_prefix: str = "gaming_summary") -> int:
    try:

        blocks_to_join = []
        counter = 1
        logging.info(f'initializing the proccess...')
        for block in pd.read_csv(file_route, chunksize=chunk_size):
            blocks_to_join.append(block)
            logging.info(f'the chunk in process is chunk # {counter} | Process rows: {len(block)}')

            counter = counter + 1

        join_blocks = pd.concat(blocks_to_join, ignore_index=True)
        general_day_daily_gaming_hours = join_blocks.groupby('gender')['daily_gaming_hours'].mean()
        general_day_anxiety_score = join_blocks.groupby('gender')['anxiety_score'].mean()
        general_day_happiness_score = join_blocks.groupby('gender')['happiness_score'].mean()
        total_players_by_gender = join_blocks.groupby('gender')['gender'].count()

        data_frame_to_export = pd.concat(
            [general_day_daily_gaming_hours, general_day_anxiety_score, general_day_happiness_score,
             total_players_by_gender], axis=1)
        data_frame_to_export.columns = ['day_daily_gaming_hours', 'day_anxiety_score', 'day_happiness_score',
                                        'total_players_by_gender']
        data_frame_to_export.to_csv(f'{output_prefix}.csv', index=True)
        logging.info(f"Process completed |  file created: {output_prefix}")

    except FileNotFoundError:
        logging.error(f"File not found: {file_route}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
    return counter


# ─────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    file_route = r'C:/Users/HP/Downloads/gaming_mental_health_10M_40features.csv'

    total = split_csv_in_chunks(
        file_route=file_route,
        chunk_size=100000,
        output_prefix="gaming_summary")
