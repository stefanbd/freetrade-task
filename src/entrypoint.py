from app.runner import ApiRunner
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Starting Runner - Fake API Runner.")
    
    runner = ApiRunner()
    try:
        runner.execute()
        logger.info("Program complete. Exiting.")
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
        raise

if __name__ == "__main__":
    main()