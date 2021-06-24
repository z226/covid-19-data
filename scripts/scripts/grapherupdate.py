import traceback

import global_vaccinations
import global_testing
import yougov
import vax_by_manufacturer
import vax_by_age
from utils.slack_client import send_error


processes = [
  global_vaccinations,
  global_testing,
  yougov,
  vax_by_manufacturer,
  vax_by_age,
]


if __name__ == "__main__":
  for process in processes:
    try:
      process.update_db()
    except Exception as e:
      tb = traceback.format_exc()
      send_error(
        channel="corona-data-updates",
        title=f'Updating Grapher dataset: {process.DATASET_NAME}',
        trace=tb,
      )