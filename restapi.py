from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@localhost:5432/postgres'
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:testing123@localhost:5432/postgres'

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Weather(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.String(10), nullable=False)
    date = db.Column(db.Date, nullable=False)
    max_temp = db.Column(db.Float)
    min_temp = db.Column(db.Float)
    precipitation = db.Column(db.Float)

    def to_dict(self):
        return {
            'id': self.id,
            'station_id': self.station_id,
            'date': self.date.isoformat(),
            'max_temp': self.max_temp,
            'min_temp': self.min_temp,
            'precipitation': self.precipitation
        }


class WeatherStats(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.String(10), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    avg_max_temp = db.Column(db.Float)
    avg_min_temp = db.Column(db.Float)
    total_precipitation = db.Column(db.Float)

    def to_dict(self):
        return {
            'id': self.id,
            'station_id': self.station_id,
            'year': self.year,
            'avg_max_temp': self.avg_max_temp,
            'avg_min_temp': self.avg_min_temp,
            'total_precipitation': self.total_precipitation
        }


@app.route('/api/weather', methods=['GET'])
def get_weather():
    query = Weather.query
    if 'date_from' in request.args:
        query = query.filter(Weather.date >= request.args['date_from'])
    if 'date_to' in request.args:
        query = query.filter(Weather.date <= request.args['date_to'])
    if 'station_id' in request.args:
        query = query.filter(Weather.station_id == request.args['station_id'])
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    pagination = query.paginate(page=page, per_page=per_page)
    return jsonify({
        'data': [weather.to_dict() for weather in pagination.items],
        'total_pages': pagination.pages,
        'current_page': page
    })


@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    query = WeatherStats.query
    if 'year' in request.args:
        query = query.filter(WeatherStats.year == int(request.args['year']))
    if 'station_id' in request.args:
        query = query.filter(WeatherStats.station_id == request.args['station_id'])
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    pagination = query.paginate(page=page, per_page=per_page)
    return jsonify({
        'data': [stats.to_dict() for stats in pagination.items],
        'total_pages': pagination.pages,
        'current_page': page
    })


if __name__ == '__main__':
    app.run(debug=True)
