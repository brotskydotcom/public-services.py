# public-services.py

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Lint](https://github.com/brotskydotcom/public-services.py/workflows/Lint/badge.svg)

This is a set of publicly-available services hosted by [Dan Brotsky Consulting](https://brotsky.com).  The exact functionality varies over time.

## Action Network/Airtable Integration

The currently posted services integrate Action Network and Airtable in support of anti-racist community organizing by the [Everyday People PAC](https://everydaypeoplepac.org/).  Triggered by Action Network web hooks, the services move people and donation records over to Airtable where the data is used by organizers in support of volunteer mobilization.

## Hosting Info

The brotsky.com public services are API-only and are rooted at [this endpoint](https://public-services.brotsky.com).  They are built in [Python](https://python.org) using [FastAPI](https://pypi.org/project/fastapi/).  They are hosted on [Heroku](https://heroku.com) using [Papertrail](https://elements.heroku.com/addons/papertrail) and [Redis Cloud](https://elements.heroku.com/addons/rediscloud) add-ons.  Service configuration files are stored in [Amazon S3](https://aws.amazon.com/s3/).

## License

The brotsky.com public services are open source and released under MIT License.  See [the LICENSE file](LICENSE) for details.