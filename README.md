Projekt: Data Engineering (DLMDWWDE02)<br>
Jan Sauerland <br>
IU Internationale Hochschule

# Anleitung
## Voraussetzungen
- Docker Desktop

## Installation & Ausführung
1. Repository klonen
2. Docker Desktop starten
3. Im Projektverzeichnis das Terminal öffnen und folgenden Befehl ausführen:
   ```bash
   docker compose -f docker-compose.yaml -p dlmdwwde02 up -d
   ```
   Warten bis die Container gestartet sind und die Logs anzeigen, dass die Services laufen.
4. Für Start des Ingestion Service folgenden Befehl ausführen:
   ```bash 
    curl.exe -H "Content-Type: application/json" -X POST -d '{\"file_path\":null}' http://localhost:8000/start
   ```
   Warten bis die Logs anzeigen, dass der Ingestion Service gestartet und alle Daten verarbeitet wurden.
5. Grafana Dashboard öffnen:
   - URL: http://localhost:3000
   - Benutzername: admin
   - Passwort: admin (Nach erstem Login muss das Passwort geändert werden)
   - Dashboard: MLB Live Game State
6. PyFlink Job starten:
   ```bash
   docker exec dlmdwwde02-mlb-jobmanager-1 ./bin/flink run -py /opt/flink/usrlib/calc.py -d
   ```
   Warten bis die Logs anzeigen, dass der PyFlink Job gestartet wurde.
7. Replay Service starten:
   ```bash
   curl.exe -H "Content-Type: application/json" -X POST http://localhost:8001/start
   ```
   Warten bis die Logs anzeigen, dass der Replay Service gestartet wurde.
8. Für Stoppen des Replay Service folgenden Befehl ausführen:
   ```bash
   curl.exe -H "Content-Type: application/json" -X POST http://localhost:8001/stop
   ```
   Warten bis die Logs anzeigen, dass der Replay Service gestoppt wurde.

# Hinweise
## Bekannte Probleme
- Aktuell Keine.
