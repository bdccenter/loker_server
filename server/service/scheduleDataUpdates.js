// server/service/scheduleDataUpdates.js (versión mejorada)
import cron from 'node-cron';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { preloadAgencyData, agencyConfig } from './bigQueryDirectService.js';
import { createConnectionWithRetry } from './dbConnection.js';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Definir rutas para logs
const LOGS_DIR = path.join(__dirname, '../../../logs');
const LOG_FILE = path.join(LOGS_DIR, 'bigquery-updates.log');

// Asegurar que existe el directorio de logs
if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR, { recursive: true });
}

/**
 * Agrega un mensaje al archivo de log con formato de fecha personalizado
 * @param {string} message - Mensaje a registrar
 */
function logMessage(message) {
  // Obtener la fecha actual
  const now = new Date();

  // Formatear la fecha en formato DD/MM/YYYY HH:MM:SS
  const day = now.getDate().toString().padStart(2, '0');
  const month = (now.getMonth() + 1).toString().padStart(2, '0');
  const year = now.getFullYear();
  const hours = now.getHours().toString().padStart(2, '0');
  const minutes = now.getMinutes().toString().padStart(2, '0');
  const seconds = now.getSeconds().toString().padStart(2, '0');

  const formattedDate = `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;

  // Crear la entrada de log con el nuevo formato
  const logEntry = `[${formattedDate}] ${message}\n`;

  try {
    fs.appendFileSync(LOG_FILE, logEntry);
  } catch (error) {
    console.error('Error al escribir en el log:', error);
  }

  // También mostrar en consola para que aparezca en los logs de Railway
  console.log(`${formattedDate} - ${message}`);
}

/**
 * Realiza la actualización de datos para todas las agencias
 * @param {string} scheduleName - Nombre de la programación (para logs)
 * @param {boolean} forceUpdate - Si se debe forzar actualización ignorando caché
 */
async function performUpdate(scheduleName = 'programada', forceUpdate = false) {
  logMessage(`Iniciando actualización ${scheduleName} de datos de BigQuery${forceUpdate ? ' (forzada)' : ''}`);

  try {
    // Obtener conexión a la base de datos
    let connection;
    try {
      // Intentar conectar con 3 reintentos, 3 segundos entre intentos
      connection = await createConnectionWithRetry({}, 3, 3000);

      // Actualizar estado de metadata a "updating"
      for (const agency of Object.keys(agencyConfig)) {
        try {
          await connection.execute(
            `INSERT INTO cache_metadata (agency, last_updated, status) 
             VALUES (?, NOW(), 'updating') 
             ON DUPLICATE KEY UPDATE last_updated = NOW(), status = 'updating'`,
            [agency]
          );
          logMessage(`Metadata actualizada para ${agency}`);
        } catch (metaError) {
          logMessage(`Error al actualizar metadata para ${agency}: ${metaError.message}`);
          // Continuar con las demás agencias
        }
      }
    } catch (dbError) {
      logMessage(`Error al conectar con la base de datos: ${dbError.message}`);
      // Continuar con la actualización de datos sin la actualización de metadata
    } finally {
      if (connection) {
        try {
          await connection.end();
          logMessage('Conexión a base de datos cerrada correctamente');
        } catch (closeError) {
          logMessage(`Error al cerrar la conexión: ${closeError.message}`);
        }
      }
    }

    // Precargar datos de todas las agencias
    await preloadAgencyData(null, forceUpdate);

    logMessage(`✅ Actualización ${scheduleName} completada correctamente`);
    return true;
  } catch (error) {
    logMessage(`❌ Error crítico en actualización ${scheduleName}: ${error.message}`);
    console.error(error);
    return false;
  }
}

/**
 * Inicializa el servicio de actualización programada
 * @param {boolean} forceInitialUpdate - Si se debe forzar la actualización inicial
 */
async function initScheduleService(forceInitialUpdate = false) {
  // Configuración de zona horaria
  const cronOptions = {
    scheduled: true,
    timezone: "America/Hermosillo" // Para Sonora, que no usa horario de verano
  };

  // Programar la tarea para ejecutarse todos los días a las 9:30 AM
  cron.schedule('30 9 * * *', () => performUpdate('diaria (9:30 AM)', true), cronOptions);

  logMessage('🚀 Servicio de actualización de datos de BigQuery iniciado');
  logMessage('📅 Programado para ejecutarse todos los días a las 9:30 AM');

  // Ejecutar una actualización inmediata al iniciar el servicio
  logMessage('⏱️ Ejecutando una actualización inicial inmediata...');

  try {
    const success = await performUpdate('inicial', forceInitialUpdate);
    if (success) {
      logMessage('✅ Actualización inicial completada con éxito.');
    } else {
      logMessage('⚠️ La actualización inicial completó con advertencias.');
    }
    logMessage('El servicio continuará ejecutándose según la programación establecida.');
    return success;
  } catch (error) {
    logMessage(`❌ Error en actualización inicial: ${error.message}`);
    logMessage('El servicio continuará intentando actualizaciones según la programación establecida.');
    throw error;
  }
}

// Manejar señales de terminación
process.on('SIGINT', () => {
  logMessage('Servicio de actualización de datos de BigQuery detenido por el usuario');
  process.exit(0);
});

process.on('SIGTERM', () => {
  logMessage('Servicio de actualización de datos de BigQuery detenido por el sistema');
  process.exit(0);
});

export { performUpdate, initScheduleService };