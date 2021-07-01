from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherVariantsUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name='COVID-19 - Variants',
            source_name=(
                f"Emma B. Hodcroft. 2021. 'CoVariants: SARS-CoV-2 Mutations and Variants of Interest.' "
                f"https://covariants.org/ – Last updated {self.time_str} (London time)"
            ),
            zero_day="2020-01-21",
        )
