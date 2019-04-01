#-*- coding: utf-8 -*-


import os
import time
import random
import datetime
import calendar
import tornado

from events import get_events
from get_track_v2 import get_data, count_fuel_consumptions
from utils import sum_consumption, xxdumps, TZ_MSK, TZ_UTC
from config import local as c

from .base import BaseHandler

__all__ = [
    'TracksHandler',
    'LengthHandler',
    'ConsumptionHandler',
    'InfoHandler'
]


class TracksHandler(BaseHandler):

    def prepare_params(self, gps_code=None):
        """Подготовка параметров, полученных из запроса"""
        self.car_id = self.get_argument('car_id', default=None)

        if gps_code:
            self.gps_code = gps_code.strip('/')
        else:
            self.gps_code = self.get_argument('gps_code', default=None)

        self.version = int(self.get_argument('version', default=2))

        # для локального теста, когда огромный трек мешает
        self.without_track = int(self.get_argument('without_track', default=0))

        self.application.logger.debug('car_id: {}, gps_code: {}, version: {}'.
                        format(self.car_id, self.gps_code, self.version))

        errors = []
        if self.car_id and self.gps_code:
            errors.append('Use only car_id or only gps_code')
        elif not (self.car_id or self.gps_code):
            errors.append('Use car_id or gps_code params')
        elif self.version < 2:
            errors.append(
                'This version of API is deprecated. Use version=2 '
                'or version=3.'
            )

        if errors:
            self.write(xxdumps({'errors': errors, }, ensure_ascii=False))
            self.finish()
            return

        self.sensors = bool(self.get_argument('sensors', default=None))
        self.format = self.get_argument('format', default='json')

        utcnow_struct = time.gmtime()
        begin_of_today_sec = calendar.timegm((
            utcnow_struct.tm_year, utcnow_struct.tm_mon, utcnow_struct.tm_mday,
            0, 0, 0
        ))
        from_dt = self.get_argument('from_dt', default=begin_of_today_sec)
        to_dt = self.get_argument('to_dt', default=time.time())
        self.from_dt = int(from_dt)
        self.to_dt = int(to_dt)

        self.from_dt_repr = datetime.datetime.fromtimestamp(self.from_dt).\
                            strftime('%Y-%m-%d %H:%M:%S')
        self.to_dt_repr = datetime.datetime.fromtimestamp(self.to_dt).\
                            strftime('%Y-%m-%d %H:%M:%S')
        self.from_dt_utc = datetime.datetime.fromtimestamp(self.from_dt).\
                            replace(tzinfo=TZ_MSK).astimezone(tz=TZ_UTC).\
                            strftime('%Y-%m-%d %H:%M:%S')
        self.to_dt_utc = datetime.datetime.fromtimestamp(self.to_dt).\
                            replace(tzinfo=TZ_MSK).astimezone(tz=TZ_UTC).\
                            strftime('%Y-%m-%d %H:%M:%S')

        self.application.logger.debug(
            'START GET TRACK gps_code: {} from_dt: {} ({}), to_dt: {} ({})'.
                format(self.gps_code, self.from_dt_repr, self.from_dt,
                        self.to_dt_repr, self.to_dt))
        return True

    @tornado.gen.coroutine
    def get_data(self, with_sensors=None, debug=False):
        data = yield get_data(
            gps_code=self.gps_code,
            car_id=self.car_id,
            from_dt=self.from_dt,
            to_dt=self.to_dt,
            db=self.application.db,
            db_vts=self.application.db_vts,
            motor_db=self.application.motor_db,
            db_aggregator2=self.application.db_aggregator2,
            db_estp=self.application.db_estp,
            redis_client=self.application.redis_client,
            with_sensors=with_sensors,
            debug=debug
        )
        return data

    @tornado.gen.coroutine
    def get(self, gps_code=None):
        params = self.prepare_params(gps_code=gps_code)
        if not params:
            self.write(xxdumps({}, ensure_ascii=False))
            self.finish()
            return

        # todo
        # временное решение для балансировки
        is_balance = self.get_argument('is_balance', '0')
        if is_balance == '1' and c.IS_BALANCED:
            url = 'http://{host}:{port}{path}'.format(
                host=c.BALANCE_HOST,
                port=random.choice(c.BALANCE_PORTS),
                path=self.request.uri
            )
            url = url.replace('is_balance=1', 'is_balance=0')

            response = yield self.application.http_client.fetch(url)
            self.write(response.body)
            self.set_status(response.code)
            self.finish()
            return

        track_data = yield self.get_data(with_sensors=self.sensors)
        track = track_data.get('track', [])
        cars_sensors = track_data.get('cars_sensors')

        self.application.logger.debug(
            'TRACK gps_code: {} from_dt: {} ({}), to_dt: {} ({}) '
            'len(track): {}'.
            format(self.gps_code, self.from_dt_repr, self.from_dt,
                    self.to_dt_repr, self.to_dt, len(track)))

        if self.version < 3:
            data = track
        else:
            data = track_data
            events = yield get_events(
                self.application.db, track, self.from_dt_utc,
                self.to_dt_utc, self.gps_code, strict=True
            )
            if events:
                data['consumptions'] = count_fuel_consumptions(
                    track, events['sensors']
                )

            data.update({
                'parkings': events.get('parkings', []),
                'sensors': cars_sensors,
                'events': events.get('sensors', {}),
                'equipment': events.get('equipment', {}),
                'equipment_distance': events.get('equipment_distance', {}),
                'equipment_time': events.get('equipment_time', {}),
                'time_of_parking': sum(
                    [p['sec'] for p in events.get('parkings', [])]),
            })

        if self.without_track:
            tmp = data.pop('track', None)
        self.write(xxdumps(data, ensure_ascii=False))
        self.finish()


class LengthHandler(TracksHandler):
    @tornado.gen.coroutine
    def get(self):
        params = self.prepare_params()
        if not params:
            self.write(xxdumps({}, ensure_ascii=False))
            self.finish()
            return

        if self.version < 2:
            json_response = xxdumps({
                'error': 'Not implemented (current version API < 2)'
            })
            self.write(json_response, ensure_ascii=False)
            self.finish()
            return

        data = yield self.get_data(with_sensors=False)

        # в метрах
        distance = int(data.get('distance') or 0)
        distance_agg2 = int(data.get('distance_agg2') or 0)

        res = {'distance': distance, 'distance_agg2': distance_agg2}

        self.write(xxdumps(res, ensure_ascii=False))
        self.finish()


class ConsumptionHandler(TracksHandler):
    @tornado.gen.coroutine
    def get(self):
        params = self.prepare_params()
        if not params:
            self.write(xxdumps({}, ensure_ascii=False))
            self.finish()
            return

        if self.version < 2:
            json_response = xxdumps({
                'error': 'Not implemented (current version API < 2)'
            })
            self.write(json_response, ensure_ascii=False)
            self.finish()
            return

        track_data = yield self.get_data(with_sensors=True,
                                            debug=self.application.debug)
        track = track_data.get('track', [])
        cars_sensors = track_data.get('cars_sensors')
        sensors_data = track_data.get('sensors_data')

        events = yield get_events(
            self.application.db, track, self.from_dt_utc,
            self.to_dt_utc, self.gps_code, strict=True
        )
        new_consumptions = {}
        if events:
            new_consumptions = count_fuel_consumptions(
                track, events['sensors']
            )

        data = {}
        data.update({
            'sensors': cars_sensors,
            'events': events.get('sensors', {}),
            'eq': events.get('eq', {}),
            'consumptions': new_consumptions,
        })

        if self.format in ['png']:
            import matplotlib.pyplot as plt

            colors = []
            for style in ['-', '-.', '--.']:
                for color in ['r', 'g', 'b']:
                    colors.append(color + style)

            ts = datetime.datetime.now().strftime('%Y-%m-%d')
            name = random.randint(1000, 9999)
            name = '/tmp/graph-{}-{}.png'.format(ts, name)
            g = None

            yy = sensors_data.get('yy')
            for sid in yy:
                plt.plot(yy[sid]['x'], yy[sid]['y'], 'gray', label='{} Original'.format(sid), linewidth=2)
                color = colors.pop() if colors else 'b'
                plt.plot(yy[sid]['x'], yy[sid]['filtered'], color, label='{} Filtered'.format(sid), linewidth=2)

                color = colors.pop() if colors else 'b'
                plt.plot(yy[sid]['x'], yy[sid]['lstsq'], color[0] + '-', label='{} LSTSQ'.format(sid), linewidth=1)

                # события на графике
                for sensor_id, sensor_events in events.get('sensors', {}).items():
                    for e in sensor_events:
                        ey = []
                        for i, x in enumerate(yy[sid]['ts']):
                            y = yy[sid]['filtered'][i]
                            if e['start_point']['timestamp'] <= x <= e['end_point']['timestamp']:
                                ey.append(y)
                            else:
                                ey.append(None)
                        color = colors.pop() if colors else 'b'
                        plt.plot(yy[sid]['x'], ey, '*', label='{} {}'.format(sid, e['type']), linewidth=4, markersize=7)

            plt.legend()
            plt.savefig(name)
            if self.application.debug:
                plt.show()
            plt.close()

            with open(name, 'rb') as f:
                g = f.read()
            os.unlink(name)

            self.set_header("Content-Type", "image/png")
            self.write(g)
            self.finish()
            return

        self.write(xxdumps(data, ensure_ascii=False))
        self.finish()


class InfoHandler(TracksHandler):
    """Возвращает информацию по заданному коду (gps_code) и временному периоду"""
    @tornado.gen.coroutine
    def get(self):
        params = self.prepare_params()
        if not params:
            self.write(xxdumps({}, ensure_ascii=False))
            self.finish()
            return

        if self.version < 2:
            json_response = xxdumps({
                'error': 'Not implemented (current version API < 2)'
            })
            self.write(json_response, ensure_ascii=False)
            self.finish()
            return

        track_data = yield self.get_data(with_sensors=True,
                                            debug=self.application.debug)

        track = track_data.get('track', [])
        cars_sensors = track_data.get('cars_sensors')
        sensors_data = track_data.get('sensors_data')

        events = yield get_events(
            self.application.db, track, self.from_dt_utc,
            self.to_dt_utc, self.gps_code, strict=True
        )
        new_consumptions = {}
        if events:
            new_consumptions = count_fuel_consumptions(
                track, events['sensors']
            )

        # рассчитываем суммарное потребление
        new_consumptions_sum = None
        if new_consumptions:
            new_consumptions_sum = sum_consumption(new_consumptions)

        # в метрах
        distance = int(track_data.get('distance') or 0)
        distance_agg2 = int(track_data.get('distance_agg2') or 0)

        data = {
            'consumption': new_consumptions_sum,
            'distance': distance,
            'distance_agg2': distance_agg2,
            'track': track,
        }
        if self.without_track:
            tmp = data.pop('track', None)
        self.write(xxdumps(data, ensure_ascii=False))
        self.finish()
