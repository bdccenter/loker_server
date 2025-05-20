// dbConnection.js - Versión segura
import mysql from 'mysql2/promise';
import 'dotenv/config'; // Aseguramos que dotenv cargue las variables de entorno

// Comprobación de variables de entorno críticas
const requiredEnvVars = ['HOST_DB', 'PORT_DB', 'USER', 'PASSWORD', 'DATABASE'];
for (const envVar of requiredEnvVars) {
  if (!process.env[envVar]) {
    console.warn(`⚠️ Variable de entorno ${envVar} no encontrada. Verifica tu archivo .env`);
  } else {
    console.log(`✅ Variable de entorno ${envVar} cargada correctamente`);
  }
}

// Verificar que hay suficientes variables de entorno definidas para continuar
const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
if (missingEnvVars.length > 0) {
  console.error(`❌ CRÍTICO: No se pueden encontrar las siguientes variables de entorno: ${missingEnvVars.join(', ')}`);
  console.error('Debes configurar todas las variables requeridas en un archivo .env o en el entorno');
  if (process.env.NODE_ENV === 'production') {
    console.error('⚠️ No se puede continuar en producción sin las variables de entorno requeridas');
    // En producción, sería mejor detener la aplicación cuando faltan variables críticas
    // process.exit(1); // Descomentar esta línea en producción
  }
}

// Crear un pool global para ser usado en toda la aplicación
const globalPool = createGlobalPool();

/**
 * Crea un pool de conexiones global que será reutilizado
 */
function createGlobalPool() {
  // Comprobamos si estamos en Railway para habilitar SSL
  const useRailway = process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY;
  
  const poolConfig = {
    host: process.env.HOST_DB,
    port: parseInt(process.env.PORT_DB || '3306', 10),
    user: process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.DATABASE,
    connectionLimit: parseInt(process.env.CONNECTION_LIMIT || '20', 10),
    acquireTimeout: 60000, // 60 segundos para adquirir conexión
    waitForConnections: true,
    queueLimit: 0,
    connectTimeout: 30000, // 30 segundos de timeout
  };

  // Habilitar SSL para Railway o cuando esté configurado explícitamente
  if (useRailway || process.env.DB_USE_SSL === 'true') {
    poolConfig.ssl = { rejectUnauthorized: false };
  }

  console.log('Creando pool global de conexiones con configuración:');
  console.log(`Host: ${poolConfig.host}`);
  console.log(`Puerto: ${poolConfig.port}`);
  console.log(`Usuario: ${poolConfig.user}`);
  console.log(`Base de datos: ${poolConfig.database}`);
  console.log(`Límite de conexiones: ${poolConfig.connectionLimit}`);
  console.log(`SSL habilitado: ${useRailway || process.env.DB_USE_SSL === 'true' ? 'Sí' : 'No'}`);

  return mysql.createPool(poolConfig);
}

/**
 * Crea una conexión a la base de datos con reintentos utilizando el pool global
 * 
 * @returns {Promise<mysql.PoolConnection>} - Conexión establecida o error
 */
export async function getDbConnection() {
  try {
    return await globalPool.getConnection();
  } catch (error) {
    console.error('Error al obtener conexión del pool:', error);
    throw error;
  }
}

/**
 * Crea una conexión a la base de datos con reintentos
 * 
 * @param {Object} config - Configuración de conexión (opcional)
 * @param {number} maxRetries - Número máximo de reintentos (default: 3)
 * @param {number} delay - Retraso entre reintentos en ms (default: 2000)
 * @returns {Promise<mysql.Connection>} - Conexión establecida o error
 */
export async function createConnectionWithRetry(config = {}, maxRetries = 3, delay = 2000) {
  // Comprobamos si estamos en Railway para habilitar SSL
  const useRailway = process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY;
  
  const connectionConfig = {
    host: config.host || process.env.HOST_DB,
    port: config.port || parseInt(process.env.PORT_DB || '3306', 10),
    user: config.user || process.env.USER,
    password: config.password || process.env.PASSWORD,
    database: config.database || process.env.DATABASE,
    connectTimeout: 30000, // 30 segundos de timeout
    ...config
  };

  // Habilitar SSL para Railway o cuando esté configurado explícitamente
  if ((useRailway || process.env.DB_USE_SSL === 'true') && !config.ssl) {
    connectionConfig.ssl = { rejectUnauthorized: false };
  }

  let lastError;
  let retries = 0;

  while (retries < maxRetries) {
    try {
      console.log(`Intentando conectar a la base de datos (intento ${retries + 1}/${maxRetries})...`);
      const connection = await mysql.createConnection(connectionConfig);
      console.log('✅ Conexión exitosa a la base de datos');
      return connection;
    } catch (error) {
      lastError = error;
      console.error(`❌ Error de conexión (intento ${retries + 1}/${maxRetries}):`, error.message);
      
      if (retries < maxRetries - 1) {
        console.log(`Reintentando en ${delay/1000} segundos...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
      
      retries++;
    }
  }

  throw new Error(`No se pudo conectar a la base de datos después de ${maxRetries} intentos: ${lastError.message}`);
}

/**
 * Crea un pool de conexiones a la base de datos con configuración personalizada
 * 
 * @param {Object} config - Configuración del pool (opcional)
 * @returns {mysql.Pool} - Pool de conexiones
 */
export function createConnectionPool(config = {}) {
  // Comprobamos si estamos en Railway para habilitar SSL
  const useRailway = process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY;
  
  const poolConfig = {
    host: config.host || process.env.HOST_DB,
    port: config.port || parseInt(process.env.PORT_DB || '3306', 10),
    user: config.user || process.env.USER,
    password: config.password || process.env.PASSWORD,
    database: config.database || process.env.DATABASE,
    connectionLimit: config.connectionLimit || parseInt(process.env.CONNECTION_LIMIT || '10', 10),
    connectTimeout: 30000, // 30 segundos de timeout
    waitForConnections: true,
    queueLimit: 0,
    ...config
  };

  // Habilitar SSL para Railway o cuando esté configurado explícitamente
  if ((useRailway || process.env.DB_USE_SSL === 'true') && !config.ssl) {
    poolConfig.ssl = { rejectUnauthorized: false };
  }

  return mysql.createPool(poolConfig);
}

/**
 * Ejecuta una consulta SQL en la base de datos con reintentos
 * 
 * @param {mysql.Pool} pool - Pool de conexiones (se usará el global si no se proporciona)
 * @param {string} sql - Consulta SQL
 * @param {Array} params - Parámetros para la consulta
 * @param {number} maxRetries - Número máximo de reintentos (default: 2)
 * @returns {Promise<Array>} - Resultados de la consulta
 */
export async function executeQueryWithRetry(pool = globalPool, sql, params = [], maxRetries = 2) {
  let lastError;
  let retries = 0;

  while (retries <= maxRetries) {
    let connection;
    try {
      connection = await pool.getConnection();
      const result = await connection.execute(sql, params);
      connection.release();
      return result;
    } catch (error) {
      if (connection) {
        connection.release();
      }
      
      lastError = error;
      console.error(`Error al ejecutar consulta (intento ${retries + 1}/${maxRetries + 1}):`, error.message);
      
      if (retries < maxRetries) {
        console.log('Reintentando...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      retries++;
    }
  }

  throw new Error(`Error al ejecutar consulta después de ${maxRetries + 1} intentos: ${lastError.message}`);
}

// Exportar el pool global para su uso en toda la aplicación
export { globalPool };