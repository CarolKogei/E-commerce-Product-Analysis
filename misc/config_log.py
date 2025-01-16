# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jumia_scraping.log'),
        logging.StreamHandler()  # Also show logs in console
    ]
)