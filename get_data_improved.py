#!/usr/bin/env python3
# Wrapper for get_data.py that uses the local improved version

import sys
import os
import fire

# Add current directory to Python path to use our local version
sys.path.insert(0, os.path.dirname(__file__))

# Import the local version with all our improvements
from qlib.tests.data import GetData

if __name__ == "__main__":
    print("Using improved qlib data download with resume capability!")
    print("Features: 256KB chunks, 2MB buffer, HTTP Range resume, 50 retries, optimized unzip")
    print()
    fire.Fire(GetData)
 