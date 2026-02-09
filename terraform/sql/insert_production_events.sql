INSERT INTO public.production_events (event_id, machine_id, event_type, event_time, duration_minutes, description) VALUES
-- Start events
(1,  1, 'start',       '2025-01-15 07:00:00', NULL, 'Morning shift start - CNC Mill 01'),
(2,  2, 'start',       '2025-01-15 07:00:00', NULL, 'Morning shift start - CNC Mill 02'),
(3,  4, 'start',       '2025-01-15 07:00:00', NULL, 'Morning shift start - Assembly Robot 01'),
(4,  7, 'start',       '2025-01-15 07:30:00', NULL, 'Morning shift start - Press Machine 01'),
(5,  8, 'start',       '2025-01-15 07:30:00', NULL, 'Morning shift start - Press Machine 02'),
(6,  1, 'start',       '2025-01-22 07:00:00', NULL, 'Morning shift start - CNC Mill 01'),
(7,  3, 'start',       '2025-01-22 07:00:00', NULL, 'Morning shift start - CNC Lathe 01'),
(8,  6, 'start',       '2025-01-22 07:15:00', NULL, 'Morning shift start - Welding Station 01'),
(9,  9, 'start',       '2025-01-29 07:00:00', NULL, 'Morning shift start - Inspection Unit 01'),
(10, 10, 'start',      '2025-02-05 07:00:00', NULL, 'Morning shift start - Packaging Unit 01'),
-- Stop events
(11, 1, 'stop',        '2025-01-15 19:00:00', NULL, 'Evening shift end - CNC Mill 01'),
(12, 2, 'stop',        '2025-01-15 19:00:00', NULL, 'Evening shift end - CNC Mill 02'),
(13, 4, 'stop',        '2025-01-15 18:30:00', NULL, 'Evening shift end - Assembly Robot 01'),
(14, 8, 'stop',        '2025-01-15 18:30:00', NULL, 'Emergency shutdown - overheating detected'),
(15, 7, 'stop',        '2025-01-22 19:00:00', NULL, 'Evening shift end - Press Machine 01'),
(16, 1, 'stop',        '2025-01-22 19:00:00', NULL, 'Evening shift end - CNC Mill 01'),
(17, 3, 'stop',        '2025-01-22 18:45:00', NULL, 'Evening shift end - CNC Lathe 01'),
(18, 6, 'stop',        '2025-01-29 19:00:00', NULL, 'Evening shift end - Welding Station 01'),
-- Maintenance events
(19, 5, 'maintenance', '2025-01-16 09:00:00', 240, 'Scheduled maintenance - servo motor replacement'),
(20, 8, 'maintenance', '2025-01-16 10:00:00', 180, 'Unplanned maintenance - cooling system repair after overheating'),
(21, 2, 'maintenance', '2025-01-23 09:00:00', 120, 'Scheduled maintenance - tool calibration and spindle check'),
(22, 10, 'maintenance','2025-01-30 08:00:00', 480, 'Major overhaul - conveyor belt and sensor replacement'),
(23, 7, 'maintenance', '2025-02-06 09:00:00', 90,  'Scheduled maintenance - hydraulic system inspection'),
-- Error events
(24, 8, 'error',       '2025-01-15 14:30:00', 45,  'Temperature sensor critical alarm - automatic shutdown triggered'),
(25, 5, 'error',       '2025-01-15 11:20:00', 30,  'Vibration anomaly detected - production paused for inspection'),
(26, 2, 'error',       '2025-01-22 13:15:00', 20,  'Spindle speed deviation - automatic correction applied'),
(27, 7, 'error',       '2025-01-22 15:00:00', 60,  'Flow rate critical drop - hydraulic line inspection required'),
-- Calibration events
(28, 1, 'calibration', '2025-01-20 06:00:00', 60,  'Monthly sensor calibration - all sensors on CNC Mill 01'),
(29, 4, 'calibration', '2025-01-27 06:00:00', 45,  'Monthly sensor calibration - all sensors on Assembly Robot 01'),
(30, 9, 'calibration', '2025-02-03 06:00:00', 30,  'Bi-weekly calibration - optical inspection sensors')
