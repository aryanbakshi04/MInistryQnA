import sys
import os
from pathlib import Path


project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


from adding_new_pdfs import main

if __name__ == "__main__":
    print("Starting Parliamentary PDF Monitor...")
    try:
        main()
        print("PDF monitoring completed successfully")
    except Exception as e:
        print(f"Error in PDF monitoring: {e}")
        sys.exit(1)
