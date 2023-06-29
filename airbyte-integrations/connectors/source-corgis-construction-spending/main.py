#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_corgis_construction_spending import SourceCorgisConstructionSpending

if __name__ == "__main__":
    source = SourceCorgisConstructionSpending()
    launch(source, sys.argv[1:])
