# etl
| branch | travis-ci | coveralls |
|--------|-----------|-----------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=master)](https://travis-ci.org/m-lab/etl) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl?branch=master) |
| integration | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=integration)](https://travis-ci.org/m-lab/etl) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=integration)](https://coveralls.io/github/m-lab/etl?branch=integration) |

| [![Waffle.io - Ready](https://badge.waffle.io/m-lab/etl.svg?label=in%20progress&title=In%20Progress)](http://waffle.io/m-lab/etl) | [![Waffle.io - In progress](https://badge.waffle.io/m-lab/etl.svg?title=Ready)](http://waffle.io/m-lab/etl) |

MeasurementLab data ingestion pipeline.

To create e.g., NDT table (should rarely be required!!!):
bq mk --time_partitioning_type=DAY --schema=schema/repeated.json mlab-sandbox:mlab_sandbox.ndt

Also see schema/README.md.
