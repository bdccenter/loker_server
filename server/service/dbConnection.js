// dbConnection.js - Nuevo archivo para gestionar conexiones
import mysql from 'mysql2/promise';

/**
 * Crea una conexión a la base de datos con reintentos
 * 
 * @param {Object} config - Configuración de conexión (opcional)
 * @param {number} maxRetries - Número máximo de reintentos (default: 3)
 * @param {number} delay - Retraso entre reintentos en ms (default: 2000)
 * @returns {Promise<mysql.Connection>} - Conexión establecida o error
 */
export async function createConnectionWithRetry(config = {}, maxRetries = 3, delay = 2000) {
  const connectionConfig = {
    host: config.host || process.env.MYSQLHOST || process.env.HOST_DB || 'localhost',
    port: config.port || process.env.MYSQLPORT || process.env.PORT_DB || 3306,
    user: config.user || process.env.MYSQLUSER || process.env.USER || 'root',
    password: config.password || process.env.MYSQLPASSWORD || process.env.PASSWORD || 'root',
    database: config.database || process.env.MYSQLDATABASE || process.env.DATABASE || 'railway',
    connectTimeout: 30000, // 30 segundos de timeout
    ssl: { rejectUnauthorized: false }, // Para conexiones a Railway
    ...config
  };

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
 * Crea un pool de conexiones a la base de datos
 * 
 * @param {Object} config - Configuración del pool (opcional)
 * @returns {mysql.Pool} - Pool de conexiones
 */
export function createConnectionPool(config = {}) {
  const poolConfig = {
    host: config.host || process.env.MYSQLHOST || process.env.HOST_DB || 'localhost',
    port: config.port || process.env.MYSQLPORT || process.env.PORT_DB || 3306,
    user: config.user || process.env.MYSQLUSER || process.env.USER || 'root',
    password: config.password || process.env.MYSQLPASSWORD || process.env.PASSWORD || 'root',
    database: config.database || process.env.MYSQLDATABASE || process.env.DATABASE || 'railway',
    connectionLimit: config.connectionLimit || process.env.CONNECTION_LIMIT || 10,
    connectTimeout: 30000, // 30 segundos de timeout
    waitForConnections: true,
    queueLimit: 0,
    ssl: { rejectUnauthorized: false }, // Para conexiones a Railway
    ...config
  };

  console.log('Creando pool de conexiones con configuración:');
  console.log(`Host: ${poolConfig.host}`);
  console.log(`Puerto: ${poolConfig.port}`);
  console.log(`Usuario: ${poolConfig.user}`);
  console.log(`Base de datos: ${poolConfig.database}`);
  console.log(`Límite de conexiones: ${poolConfig.connectionLimit}`);

  return mysql.createPool(poolConfig);
}

/**
 * Ejecuta una consulta SQL en la base de datos con reintentos
 * 
 * @param {mysql.Pool} pool - Pool de conexiones
 * @param {string} sql - Consulta SQL
 * @param {Array} params - Parámetros para la consulta
 * @param {number} maxRetries - Número máximo de reintentos (default: 2)
 * @returns {Promise<Array>} - Resultados de la consulta
 */
export async function executeQueryWithRetry(pool, sql, params = [], maxRetries = 2) {
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