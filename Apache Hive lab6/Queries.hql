-- Queries.hql : Requêtes analytiques et de nettoyage
USE hotel_booking;

-- 5. Requêtes simples
SELECT * FROM clients;
SELECT * FROM hotels WHERE ville = 'Paris';
SELECT r.*, c.nom AS client_nom, h.nom AS hotel_nom
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id;

-- 6. Requêtes avec jointures
SELECT client_id, COUNT(*) AS nb_reservations FROM reservations GROUP BY client_id;
SELECT client_id FROM reservations WHERE DATEDIFF(date_fin, date_debut) > 2 GROUP BY client_id;
SELECT r.client_id, h.nom AS hotel_nom FROM reservations r JOIN hotels h ON r.hotel_id = h.hotel_id;
SELECT h.nom FROM reservations r JOIN hotels h ON r.hotel_id = h.hotel_id GROUP BY h.nom HAVING COUNT(*) > 1;
SELECT h.nom FROM hotels h LEFT JOIN reservations r ON h.hotel_id = r.hotel_id WHERE r.hotel_id IS NULL;

-- 7. Requêtes imbriquées
SELECT DISTINCT c.nom FROM reservations r JOIN hotels h ON r.hotel_id = h.hotel_id JOIN clients c ON r.client_id = c.client_id WHERE h.etoiles > 4;
SELECT hotel_id, SUM(prix_total) AS total_revenu FROM reservations GROUP BY hotel_id;

-- 8. Agrégations avec partitions et buckets
SELECT h.ville, SUM(r.prix_total) AS total_revenu FROM reservations r JOIN hotels h ON r.hotel_id = h.hotel_id GROUP BY h.ville;
SELECT client_id, COUNT(*) AS nb_resa FROM reservations_bucketed GROUP BY client_id;

-- 9. Nettoyage
DROP TABLE IF EXISTS clients;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS hotels_partitioned;
DROP TABLE IF EXISTS reservations_bucketed;
