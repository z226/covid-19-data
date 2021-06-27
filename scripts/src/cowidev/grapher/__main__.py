import traceback

from cowidev.grapher.procs.vax import GrapherVaxUpdater
from cowidev.grapher.procs.testing import GrapherTestUpdater
from cowidev.grapher.procs.yougov import GrapherYougovUpdater
from cowidev.grapher.procs.vax_age import GrapherVaxAgeUpdater
from cowidev.grapher.procs.vax_manufacturer import GrapherVaxManufacturerUpdater
from cowidev.grapher.utils.slack_client import send_error


updaters = [
  GrapherVaxUpdater,
  GrapherTestUpdater,
  GrapherYougovUpdater,
  GrapherVaxAgeUpdater,
  GrapherVaxManufacturerUpdater,
]
updaters = [u() for u in updaters]


def main():
    for updater in updaters:
        try:
            updater.run()
        except Exception as e:
            tb = traceback.format_exc()
            send_error(
                channel="corona-data-updates",
                title=f'Updating Grapher dataset: {updater.dataset_name}',
                trace=tb,
            )
