import json
from typing import Dict, List, Any

from bokeh.client import push_session, ClientSession
from bokeh.io import curdoc
from bokeh.layouts import row
from bokeh.models import GlyphRenderer
from bokeh.plotting import figure, Figure
from kafka import KafkaConsumer


class RankedCountVisualisation:
    def __init__(self, path_to_configs: str):
        self.bokeh_session: ClientSession = push_session(curdoc())
        configs: Dict[str, Any] = self.read_configs(path_to_configs)
        self.consumer: KafkaConsumer = KafkaConsumer(configs["topic"],
                                                     bootstrap_servers=configs["bootstrap_servers"],
                                                     auto_offset_reset=configs["auto_offset"],
                                                     enable_auto_commit=True,
                                                     group_id=configs["group_id"],
                                                     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

        self.ranked_count: Dict[str, int] = {}
        self.MAX_POLL_RECORDS: int = 1

    @staticmethod
    def read_configs(path: str) -> Dict[str, Any]:
        with open(path) as f:
            return json.load(f)

    def visualize(self):
        bar_fig, circle_fig = self.get_plot(list(map(str, range(1, 11)))), self.get_plot(list(map(str, range(1, 11))))
        bar_chart = self.create_bar_plot(bar_fig)
        circle_chart = self.create_circle_plot(circle_fig)
        curdoc().add_periodic_callback(lambda: self.update_plot(bar_chart, circle_chart), 3000)
        self.bokeh_session.show(row(bar_fig, circle_fig))
        self.bokeh_session._loop_until_closed()

    @staticmethod
    def create_bar_plot(plot=None) -> GlyphRenderer:
        plot.xgrid.grid_line_color = None
        plot.y_range.start = 0
        bar_chart: GlyphRenderer = plot.vbar(x=[], top=[], width=0.8)
        return bar_chart

    @staticmethod
    def create_circle_plot(plot=None) -> GlyphRenderer:
        plot.xgrid.grid_line_color = None
        plot.y_range.start = 0
        circle_chart: GlyphRenderer = plot.circle(x=[], y=[], size=10, color="navy", alpha=0.5)
        return circle_chart

    @staticmethod
    def get_plot(x_range: List[Any], h=700, w=700) -> Figure:
        return figure(x_range=x_range,
                      height=h, width=w,
                      title="PBM ranks clicks",
                      toolbar_location=None)

    def update_plot(self, bar, circle):
        mc = 0
        for message in self.consumer:
            message = message.value
            rank = message["rank"]
            count = message["count"]
            self.ranked_count[rank] = count
            bar.data_source.data = {"x": list(map(str, self.ranked_count.keys())),
                                    "top": list(self.ranked_count.values())}
            circle.data_source.data = {"x": list(map(str, self.ranked_count.keys())),
                                       "y": list(self.ranked_count.values())}
            if mc + 1 > self.MAX_POLL_RECORDS:
                break
            else:
                mc += 1


ranked_visualisation: RankedCountVisualisation = RankedCountVisualisation("configs/kafka-consumer-config.json")
ranked_visualisation.visualize()