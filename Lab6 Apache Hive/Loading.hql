-- Loading.hql : Chargement des données dans les tables Hive
USE hotel_booking;

LOAD DATA LOCAL INPATH '/shared_volume/clients.txt' INTO TABLE clients;
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' INTO TABLE hotels;
LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' INTO TABLE reservations;
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' INTO TABLE hotels_partitioned;
LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' INTO TABLE reservations_bucketed;
