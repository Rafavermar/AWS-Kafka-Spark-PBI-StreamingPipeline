import os
import random
import uuid
import gpxpy.gpx
import simplejson as json
from confluent_kafka import SerializingProducer
import gpxpy
import gpxpy.gpx
import json
import requests
import time
from datetime import datetime, timedelta
from geopy.distance import geodesic
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOSTRAP_SERVERS = os.getenv('KAFKA_BOOSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
FAILURES_TOPIC = os.getenv('FAILURES_TOPIC', 'failures_data')

random.seed(42)
start_time = datetime.now()


def get_extension_value(extensions, tag_name, namespace):
    ns_tag = f"{{{namespace}}}{tag_name}"
    for extension in extensions:
        if extension.tag == ns_tag:
            return extension.text
    return None


MYTRACKS_NAMESPACE = "http://mytracks.stichling.info/myTracksGPX/1/0"


def read_gpx_file(file_path):
    realtime_points = []
    with open(file_path, 'r') as gpx_file:
        gpx = gpxpy.parse(gpx_file)
        for track in gpx.tracks:
            for segment in track.segments:
                for point in segment.points:

                    speed = get_extension_value(point.extensions, 'speed', MYTRACKS_NAMESPACE)
                    length = get_extension_value(point.extensions, 'length', MYTRACKS_NAMESPACE)

                    if speed is not None:
                        speed = float(speed)
                    if length is not None:
                        length = float(length)

                    point_info = {
                        'latitude': point.latitude,
                        'longitude': point.longitude,
                        'elevation': point.elevation,
                        'time': point.time.isoformat() if point.time else None,
                        'speed': speed,
                        'length': length
                    }
                    realtime_points.append(point_info)
    return realtime_points


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time


OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '')
OPENWEATHER_API_URL = os.getenv('OPENWEATHER_API_URL', '')
OPENAIRQUALITY_API_URL = os.getenv('OPENAIRQUALITY_API_URL', '')

last_weather_fetch_time = None
last_weather_data = None
last_air_quality_data = None


def generate_weather_and_air_quality_data(device_id, timestamp, latitude, longitude, api_key):
    global last_weather_fetch_time, last_weather_data, last_air_quality_data
    current_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

    if last_weather_fetch_time is None or (current_time - last_weather_fetch_time) >= timedelta(minutes=10):
        last_weather_fetch_time = current_time

        weather_url = f"{OPENWEATHER_API_URL}?lat={latitude}&lon={longitude}&appid={api_key}&units=metric"
        weather_response = requests.get(weather_url)
        if weather_response.status_code == 200:
            weather_data = weather_response.json()

            temperature = weather_data["main"]["temp"]
            weather_condition = weather_data["weather"][0]["main"]
            wind_speed = weather_data["wind"]["speed"]
            humidity = weather_data["main"]["humidity"]

            last_weather_data = {
                'temperature': temperature,
                'weatherCondition': weather_condition,
                'windSpeed': wind_speed,
                'humidity': humidity,
            }
        else:
            print("Error al obtener datos climáticos:", weather_response.status_code)

        air_quality_url = f"{OPENAIRQUALITY_API_URL}?lat={latitude}&lon={longitude}&appid={api_key}"
        air_quality_response = requests.get(air_quality_url)
        if air_quality_response.status_code == 200:
            air_quality_data = air_quality_response.json()

            air_quality_index = air_quality_data["list"][0]["main"]["aqi"]

            last_air_quality_data = {
                'airQualityIndex': air_quality_index,

            }
        else:
            print("Error obtaining air quality data:", air_quality_response.status_code)

        if last_weather_data and last_air_quality_data:
            combined_data = {**last_weather_data, **last_air_quality_data, 'id': device_id, 'timestamp': timestamp}
            return combined_data
        else:
            return {}
    return {**last_weather_data, **last_air_quality_data,
            'id': device_id, 'timestamp': timestamp} if last_weather_data and last_air_quality_data else {}


SUSPENSION_REASONS = [
    'Failure to stop',
    'Yield sign ignored',
    'Amber light not respected',
    'Pedestrian crossing not respected',
    'Wrong way taken'
]

CENTRAL_POINT = (36.70828195217619, -4.465170324163806)
RADIUS_KM = 1.0


def is_within_radius(point, center=CENTRAL_POINT, radius=RADIUS_KM):
    return geodesic(point, center).km <= radius


last_suspension_time = None


def generate_test_drive_failure_data(device_id, timestamp, location):
    global last_suspension_time
    current_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

    failure_data = {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'incidentId': uuid.uuid4(),
        'type': 'Test Drive Suspension',
        'timestamp': timestamp,
        'location': location,
        'description': None,
        'num_failures': 0,
    }

    if is_within_radius((location['latitude'], location['longitude'])) and \
            (last_suspension_time is None or (current_time - last_suspension_time) >= timedelta(minutes=5)):
        last_suspension_time = current_time
        failure_reason = random.choice(SUSPENSION_REASONS)
        num_failures = random.randint(1, 10)

        failure_data['description'] = f'{num_failures} suspensions: {failure_reason}'
        failure_data['num_failures'] = num_failures

    return failure_data


gpx_points = read_gpx_file('data/2022-12-02 12_12_41.gpx')


def generate_vehicle_data(device_id, point):
    speed = point['speed']

    length = point['length'] if 'length' in point and point['length'] is not None else 0

    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': point['time'],
        'location': {'latitude': point['latitude'], 'longitude': point['longitude']},
        'speed': speed,
        'length': length,
        'elevation': point['elevation'],
        'make': 'Yamaha',
        'model': 'MT-07',
        'year': 2024,
        'fuelType': 'Gasoline'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not Json serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()


def simulate_journey(producer, device_id, realtime_points, api_key):
    global last_weather_fetch_time

    for point in realtime_points:
        vehicle_data = generate_vehicle_data(device_id, point)
        weather_data = generate_weather_and_air_quality_data(device_id, point['time'], point['latitude'],
                                                             point['longitude'], api_key)

        failures_data = generate_test_drive_failure_data(device_id, point['time'],
                                                         {'latitude': point['latitude'],
                                                          'longitude': point['longitude']})
        if failures_data is not None:
            produce_data_to_kafka(producer, FAILURES_TOPIC, failures_data)

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)

        current_time_index = realtime_points.index(point)
        if current_time_index < len(realtime_points) - 1:
            next_point_time = datetime.strptime(realtime_points[current_time_index + 1]['time'],
                                                "%Y-%m-%dT%H:%M:%S.%f%z")
            current_point_time = datetime.strptime(point['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
            time_to_next_point = (next_point_time - current_point_time).total_seconds()

            time.sleep(min(time_to_next_point, 2))

    print('Simulation completed. All the GPX points have been processed.')


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    gpx_file_path = 'data/2022-12-02 12_12_41.gpx'
    gpx_points = read_gpx_file(gpx_file_path)

    try:
        simulate_journey(producer, 'Vehicle-Rvm-001', gpx_points, OPENWEATHER_API_KEY)
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
