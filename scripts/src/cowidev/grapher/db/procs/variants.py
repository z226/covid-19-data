from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherVariantsUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name='COVID-19 - Variants.csv',
            source_name=(
                f"CoVariants: SARS-CoV-2 Mutations and Variants of Interest – Last updated {self.time_str} "
                f"(London time)"
            ),
            zero_day="2020-01-21",
        )
