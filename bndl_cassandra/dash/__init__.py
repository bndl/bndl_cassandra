# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bndl.util import dash
from bndl.util.exceptions import catch
from bndl_cassandra.metrics import get_cassandra_metrics, metrics_by_cluster
from flask.blueprints import Blueprint
from flask.globals import g
from flask.templating import render_template


blueprint = Blueprint('cassandra', __name__,
                      template_folder='templates')


class Status(dash.StatusPanel):
    def render(self):
        metrics, by_cluster = _get_metrics()
        status = dash.status.OK if by_cluster else dash.status.DISABLED
        return status, render_template('cassandra/status.html',
                                       metrics=by_cluster,
                                       metric_provider_count=len(metrics))


class Dash(dash.Dash):
    blueprint = blueprint
    status_panel_cls = Status


def _get_metrics():
    metrics = [get_cassandra_metrics()]
    requests = [worker.execute(get_cassandra_metrics) for worker in g.ctx.workers]
    for request in requests:
        with catch():
            metrics.append(request.result())

    by_cluster = metrics_by_cluster(metrics)
    return metrics, by_cluster


@blueprint.route('/')
def index():
    metrics, by_cluster = _get_metrics()
    return render_template('cassandra/dashboard.html',
                           metrics=by_cluster,
                           metric_provider_count=len(metrics))
