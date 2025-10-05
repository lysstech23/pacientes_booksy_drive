# Pacientes Booksy Drive

Este repositorio contiene un script en Python que lee correos electrónicos de `no-reply@booksy.com` en una cuenta de Gmail, extrae los datos de los pacientes (Nombre, Apellidos, Teléfono y Email) y los guarda en un Google Sheet llamado **directorio_pacientes**.

## Características

* **Ejecución inicial**: procesa todo el historial de correos del remitente indicado.
* **Ejecuciones recurrentes**: cada hora, solo procesa correos nuevos (no procesados previamente).
* **Antiduplicados**: evita insertar pacientes repetidos basándose en el ID del mensaje de Gmail y en el teléfono/email del paciente.
* **Parser flexible**: maneja distintas variantes de las notificaciones de reserva de Booksy y extrae los campos de forma robusta.
* **Configuración via variables de entorno**: permite ejecutar en local o en CI (por ejemplo en GitHub Actions) con autenticación OAuth.

## Requisitos

1. Habilitar las APIs de **Gmail** y **Google Sheets** en Google Cloud.
2. Crear credenciales OAuth (tipo desktop para modo local, tipo web para CI) y obtener un *refresh token* para la cuenta de Gmail.
3. Python 3.10 o superior.

## Uso en local

1. Instalar dependencias:

   ```bash
   pip install -r requirements.txt
   ```
2. Colocar un fichero `credentials.json` (client OAuth) en el directorio raíz.
3. Ejecutar el script:

   ```bash
   python src/booksy_gmail_to_sheets.py
   ```
   La primera vez se abrirá el navegador para autorizar los scopes. Se generará un `token.json` para usos futuros.

## Uso en CI (GitHub Actions)

1. Obtener el **refresh token** ejecutando el script en local y copiando del fichero `token.json` los campos `client_id`, `client_secret` y `refresh_token`.
2. Crear los secrets en GitHub:
   * `GOOGLE_CLIENT_ID`
   * `GOOGLE_CLIENT_SECRET`
   * `GOOGLE_REFRESH_TOKEN`
   * `GOOGLE_SHEETS_SPREADSHEET_ID` (opcional si se quiere usar una hoja existente)
3. Ajustar, si se desea, el título de la hoja con la variable `GOOGLE_SHEETS_TITLE` (por defecto: `directorio_pacientes`).
4. El workflow `.github/workflows/hourly.yml` ejecutará el script cada hora.

## Estructura del repositorio

```
pacientes_booksy_drive/
├── src/
│   └── booksy_gmail_to_sheets.py      # Script principal
├── requirements.txt                   # Dependencias de Python
├── README.md                          # Este documento
└── .github/
    └── workflows/
        └── hourly.yml                 # Workflow de GitHub Actions
```

## Notas técnicas

El script extrae los datos convirtiendo el contenido HTML del correo a texto mediante BeautifulSoup y aplicando expresiones regulares para detectar emails y teléfonos. Para deduplicar pacientes, consulta las columnas ya existentes en la hoja de Google y evita insertar entradas con el mismo teléfono o email. Además registra el ID del mensaje procesado en una pestaña meta llamada `_meta_processed_messages`.

El horario de ejecución puede modificarse editando el campo `cron` en el workflow. Por defecto se ejecuta a la hora en punto, cada hora.