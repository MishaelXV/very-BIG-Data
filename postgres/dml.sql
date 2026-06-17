-- Idempotent: ON CONFLICT DO NOTHING makes it safe to re-run
INSERT INTO device_types (id, type_name, description)
VALUES
    (1, 'temperature_sensor', 'Measures ambient air temperature in degrees Celsius'),
    (2, 'humidity_sensor',    'Measures relative humidity as a percentage'),
    (3, 'pressure_sensor',    'Measures atmospheric pressure in hPa'),
    (4, 'co2_sensor',         'Measures CO2 concentration in the air in ppm'),
    (5, 'motion_sensor',      'Detects presence and movement in a monitored area')
ON CONFLICT (id) DO NOTHING;
