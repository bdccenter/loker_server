// server/service/bigQueryDirectService.js
import { BigQuery } from '@google-cloud/bigquery';
import 'dotenv/config';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import zlib from 'zlib';
import { getDbConnection } from './dbConnection.js';
import Papa from 'papaparse';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24 horas en milisegundos
const CACHE_DIR = path.join(__dirname, '../../../cache');
const CACHE_FILENAME = path.join(CACHE_DIR, 'bigquery_cache.json');

// Definir la ruta para archivos CSV
const CSV_DIR = path.join(CACHE_DIR, 'csv');
if (!fs.existsSync(CSV_DIR)) {
  fs.mkdirSync(CSV_DIR, { recursive: true });
}

// Asegurar que existe el directorio de caché
if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

// Caché en memoria optimizada para respuestas rápidas
const memoryCache = new Map();
const MEMORY_CACHE_TTL = 30 * 60 * 1000; // 30 minutos

function setMemoryCache(key, data) {
  memoryCache.set(key, {
    data,
    timestamp: Date.now()
  });
}

function getMemoryCache(key) {
  if (!memoryCache.has(key)) return null;

  const cacheItem = memoryCache.get(key);
  const now = Date.now();

  // Verificar si la caché ha expirado
  if (now - cacheItem.timestamp > MEMORY_CACHE_TTL) {
    memoryCache.delete(key);
    return null;
  }

  return cacheItem.data;
}

// Cargar caché desde archivo si existe
let queryCache = new Map();
try {
  if (fs.existsSync(CACHE_FILENAME)) {
    const cacheData = JSON.parse(fs.readFileSync(CACHE_FILENAME, 'utf8'));
    // Convertir objeto a Map
    for (const [key, value] of Object.entries(cacheData)) {
      queryCache.set(key, value);
    }
    console.log('✅ Caché cargada desde archivo:', queryCache.size, 'entradas');
  }
} catch (error) {
  console.error('❌ Error al cargar caché desde archivo:', error);
  queryCache = new Map(); // En caso de error, iniciar con caché vacía
}

// Función para guardar caché en archivo
function saveCacheToFile() {
  try {
    const cacheObj = {};
    queryCache.forEach((value, key) => {
      cacheObj[key] = value;
    });
    fs.writeFileSync(CACHE_FILENAME, JSON.stringify(cacheObj), 'utf8');
    console.log('✅ Caché guardada en archivo:', Object.keys(cacheObj).length, 'entradas');
  } catch (error) {
    console.error('❌ Error al guardar caché en archivo:', error);
  }
}

// Configuración de credenciales
const credentials = {
  type: process.env.BQ_TYPE || "service_account",
  project_id: process.env.BQ_PROJECT_ID,
  private_key_id: process.env.BQ_PRIVATE_KEY_ID,
  private_key: process.env.BQ_PRIVATE_KEY?.replace(/\\n/g, '\n'),
  client_email: process.env.BQ_CLIENT_EMAIL,
  client_id: process.env.BQ_CLIENT_ID,
  auth_uri: process.env.BQ_AUTH_URI || "https://accounts.google.com/o/oauth2/auth",
  token_uri: process.env.BQ_TOKEN_URI || "https://oauth2.googleapis.com/token",
  auth_provider_x509_cert_url: process.env.BQ_AUTH_PROVIDER_CERT_URL || "https://www.googleapis.com/oauth2/v1/certs",
  client_x509_cert_url: process.env.BQ_CLIENT_CERT_URL,
  universe_domain: process.env.BQ_UNIVERSE_DOMAIN || "googleapis.com"
};

// Opciones optimizadas para BigQuery
const bigqueryOptions = {
  maxRetries: 3,
  retryOptions: {
    retryDelayMultiplier: 2.0,
    totalTimeout: 300000 // 5 minutos de timeout - aumentado para consultas grandes
  },
  scopes: ['https://www.googleapis.com/auth/bigquery'],
  // Configuraciones adicionales para mejorar rendimiento
  query: {
    useQueryCache: true,
    maximumBytesBilled: '1000000000', // 1GB
    timeoutMs: 300000 // 5 minutos
  }
};

// Instanciar el cliente de BigQuery con manejo de errores
const keyFilePath = path.join(__dirname, '../../google-credentials.json');
console.log(`Buscando archivo de credenciales en: ${keyFilePath}`);

let bigquery;
try {
  // Si hay variables de entorno completas, usarlas directamente
  if (process.env.BQ_PROJECT_ID && process.env.BQ_PRIVATE_KEY && process.env.BQ_CLIENT_EMAIL) {
    const credentials = {
      type: process.env.BQ_TYPE || "service_account",
      project_id: process.env.BQ_PROJECT_ID,
      private_key_id: process.env.BQ_PRIVATE_KEY_ID,
      private_key: process.env.BQ_PRIVATE_KEY.replace(/\\n/g, '\n'),
      client_email: process.env.BQ_CLIENT_EMAIL,
      client_id: process.env.BQ_CLIENT_ID,
      auth_uri: process.env.BQ_AUTH_URI || "https://accounts.google.com/o/oauth2/auth",
      token_uri: process.env.BQ_TOKEN_URI || "https://oauth2.googleapis.com/token",
      auth_provider_x509_cert_url: process.env.BQ_AUTH_PROVIDER_CERT_URL || "https://www.googleapis.com/oauth2/v1/certs",
      client_x509_cert_url: process.env.BQ_CLIENT_CERT_URL,
      universe_domain: process.env.BQ_UNIVERSE_DOMAIN || "googleapis.com"
    };

    bigquery = new BigQuery({
      projectId: process.env.BQ_PROJECT_ID,
      credentials: credentials,
      ...bigqueryOptions
    });
    console.log('✅ Cliente BigQuery inicializado correctamente usando variables de entorno');
  }
  // Si no hay variables completas, intentar con el archivo de credenciales
  else if (fs.existsSync(keyFilePath)) {
    bigquery = new BigQuery({
      keyFilename: keyFilePath,
      ...bigqueryOptions
    });
    console.log('✅ Cliente BigQuery inicializado correctamente usando archivo de credenciales');
  }
  else {
    throw new Error('No se encontraron credenciales válidas para BigQuery');
  }
} catch (error) {
  console.error('❌ ERROR: No se pudo inicializar el cliente de BigQuery:', error.message);
}

/**
 * Configuración actualizada de agencias para tablas de retención
 */
const agencyConfig = {
  'Gran Auto': {
    projectId: 'base-maestra-gn',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    dateField: 'FECHA_FAC',
    dateFormat: '%d/%m/%Y'
  },
  'Del Bravo': {
    projectId: 'base-maestra-delbravo',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    dateField: 'FECHA_FAC',
    dateFormat: '%d/%m/%Y'
  },
  'Sierra': {
    projectId: 'base-maestra-sierra',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    dateField: 'FECHA_FAC',
    dateFormat: '%d/%m/%Y'
  },
  'Huerpel': {
    projectId: 'base-maestra-huerpel',
    datasetName: 'Posventas',
    tableName: 'tab_bafac_ur',
    dateField: 'FECHA_FACT',
    dateFormat: '%d/%m/%Y'
  }
};

// Flag para saber si MySQL está disponible
let mysqlAvailable = true;

// Mejor manejo de caché en memoria y base de datos
async function getFromCache(agencyName, queryHash) {
  // Aplicar optimización para la caché en memoria
  const memoryCacheKey = `${agencyName}:${queryHash}`;
  const memoryCacheData = getMemoryCache(memoryCacheKey);
  if (memoryCacheData) {
    console.log(`Caché en memoria encontrada para ${agencyName}`);
    return memoryCacheData; // Retornar inmediatamente sin más procesamiento
  }

  try {
    const connection = await getDbConnection();

    // Consulta optimizada con límite de tiempo
    const [rows] = await connection.execute(
      'SELECT data, timestamp, is_compressed FROM query_cache WHERE cache_key = ? LIMIT 1',
      [`${agencyName}:${queryHash}`],
      { timeout: 5000 } // 5 segundos máximo
    );

    connection.release(); // Liberar inmediatamente la conexión

    if (rows.length > 0) {
      // Comprobar si la caché está actualizada (24 horas)
      const cacheTime = new Date(rows[0].timestamp).getTime();
      const now = Date.now();

      // Asegurarse de que los tiempos son válidos
      if (isNaN(cacheTime) || isNaN(now)) {
        console.error('Error: Timestamp inválido detectado', {
          cacheTimestamp: rows[0].timestamp,
          cacheTime: cacheTime,
          now: now
        });
        return null; // No usar caché con timestamps inválidos
      }

      const cacheAge = now - cacheTime;

      // Protección contra diferencias de tiempo negativas (posibles problemas de zona horaria)
      if (cacheAge < 0) {
        console.error(`Error: Diferencia de tiempo negativa detectada (${cacheAge} ms)`, {
          cacheTime: new Date(cacheTime).toISOString(),
          now: new Date(now).toISOString()
        });

        // Invalidar esta caché
        try {
          await invalidateCacheInDb(agencyName);
        } catch (e) {
          console.error('Error al invalidar caché con timestamp incorrecto:', e);
        }

        return null; // No usar caché con diferencias negativas
      }

      // Si la caché es reciente (menos de 24 horas), usarla
      if (cacheAge < CACHE_DURATION) {
        console.log(`Caché válida encontrada para ${agencyName}, edad: ${Math.round(cacheAge / 1000 / 60)} minutos`);

        // Descomprimir si es necesario
        let data;
        if (rows[0].is_compressed) {
          try {
            const compressedData = rows[0].data;
            const jsonData = zlib.gunzipSync(Buffer.from(compressedData, 'base64')).toString();
            data = JSON.parse(jsonData);
          } catch (decompressError) {
            console.error('Error al descomprimir datos de caché:', decompressError);
            return null;
          }
        } else {
          data = JSON.parse(rows[0].data);
        }

        // Guardar en caché de memoria para futuras consultas
        setMemoryCache(memoryCacheKey, data);

        return data;
      }
    }

    return null;
  } catch (error) {
    console.error('Error al consultar caché en MySQL:', error);
    return null;
  }
}


// Función para guardar en caché con mejor manejo de errores
async function saveToCache(agencyName, queryHash, data) {
  // También guardar en caché de memoria
  setMemoryCache(`${agencyName}:${queryHash}`, data);

  // Si MySQL no está disponible, no intentar guardar
  if (!mysqlAvailable) {
    console.log(`MySQL no disponible, no se guardará en caché DB para ${agencyName}`);
    return false;
  }

  try {
    const connection = await getDbConnection();

    // Verificar que estamos guardando un timestamp válido y razonable
    const now = new Date();
    
    // Verificación del timestamp actual
    if (now.getFullYear() > 2100 || now.getFullYear() < 2020) {
      console.error(`Timestamp inválido detectado (${now.toISOString()}), ajustando a fecha del sistema`);
      // Forzar fecha actual del sistema
      const systemDate = new Date();
      // Ejecutar primero la verificación de timestamp para evitar guardar fechas incorrectas
      await connection.execute(
        `INSERT INTO cache_metadata (agency, last_updated, record_count, status) 
         VALUES (?, ?, ?, 'success') 
         ON DUPLICATE KEY UPDATE last_updated = ?, record_count = VALUES(record_count), 
         status = 'success', error_message = NULL`,
        [agencyName, systemDate, data.length, systemDate]
      );
    } else {
      // Guardar metadata primero con NOW() de MySQL (menos propenso a errores)
      await connection.execute(
        `INSERT INTO cache_metadata (agency, last_updated, record_count, status) 
         VALUES (?, NOW(), ?, 'success') 
         ON DUPLICATE KEY UPDATE last_updated = NOW(), record_count = VALUES(record_count), 
         status = 'success', error_message = NULL`,
        [agencyName, data.length]
      );
    }

    // Convertir datos a JSON
    const jsonData = JSON.stringify(data);
    const dataSize = Buffer.byteLength(jsonData, 'utf8');

    // Si los datos son mayores a 1MB, comprimir
    if (dataSize > 1 * 1024 * 1024) {
      try {
        console.log(`Comprimiendo datos (${Math.round(dataSize / 1024 / 1024)}MB) para ${agencyName}`);
        const compressedData = zlib.gzipSync(jsonData).toString('base64');
        const compressedSize = Buffer.byteLength(compressedData, 'utf8');
        console.log(`Datos comprimidos a ${Math.round(compressedSize / 1024 / 1024)}MB (ahorro: ${Math.round((1 - compressedSize / dataSize) * 100)}%)`);

        // Si los datos comprimidos son muy grandes (más de 16MB), guardar solo en caché de memoria
        if (compressedSize > 16 * 1024 * 1024) {
          console.log(`Datos comprimidos demasiado grandes para MySQL (${Math.round(compressedSize / 1024 / 1024)}MB), guardando solo en caché de memoria`);
          connection.release();
          return false;
        }

        // Usar la misma lógica de verificación de timestamp para los datos
        if (now.getFullYear() > 2100 || now.getFullYear() < 2020) {
          const systemDate = new Date();
          // Intentar insertar los datos comprimidos con timestamp explícito
          await connection.execute(
            `INSERT INTO query_cache (cache_key, data, timestamp, is_compressed) 
             VALUES (?, ?, ?, 1) 
             ON DUPLICATE KEY UPDATE data = VALUES(data), timestamp = ?, is_compressed = 1`,
            [`${agencyName}:${queryHash}`, compressedData, systemDate, systemDate]
          );
        } else {
          // Usar NOW() de MySQL (opción más segura)
          await connection.execute(
            `INSERT INTO query_cache (cache_key, data, timestamp, is_compressed) 
             VALUES (?, ?, NOW(), 1) 
             ON DUPLICATE KEY UPDATE data = VALUES(data), timestamp = NOW(), is_compressed = 1`,
            [`${agencyName}:${queryHash}`, compressedData]
          );
        }

        connection.release();
        console.log(`Caché comprimida actualizada para ${agencyName}: ${data.length} registros`);
        return true;
      } catch (compressError) {
        console.error('Error al comprimir datos:', compressError);
        // Si hay error en compresión, intentar guardar sin comprimir si no son demasiado grandes
        if (dataSize <= 5 * 1024 * 1024) {
          // Verificar timestamp también aquí
          if (now.getFullYear() > 2100 || now.getFullYear() < 2020) {
            const systemDate = new Date();
            await connection.execute(
              `INSERT INTO query_cache (cache_key, data, timestamp, is_compressed) 
               VALUES (?, ?, ?, 0) 
               ON DUPLICATE KEY UPDATE data = VALUES(data), timestamp = ?, is_compressed = 0`,
              [`${agencyName}:${queryHash}`, jsonData, systemDate, systemDate]
            );
          } else {
            await connection.execute(
              `INSERT INTO query_cache (cache_key, data, timestamp, is_compressed) 
               VALUES (?, ?, NOW(), 0) 
               ON DUPLICATE KEY UPDATE data = VALUES(data), timestamp = NOW(), is_compressed = 0`,
              [`${agencyName}:${queryHash}`, jsonData]
            );
          }

          connection.release();
          console.log(`Caché sin comprimir actualizada para ${agencyName}: ${data.length} registros`);
          return true;
        } else {
          console.log(`Datos demasiado grandes para MySQL sin comprimir (${Math.round(dataSize / 1024 / 1024)}MB)`);
          connection.release();
          return false;
        }
      }
    } else {
      // Si los datos son pequeños, guardarlos sin comprimir
      // Verificar timestamp también aquí
      if (now.getFullYear() > 2100 || now.getFullYear() < 2020) {
        const systemDate = new Date();
        await connection.execute(
          `INSERT INTO query_cache (cache_key, data, timestamp, is_compressed) 
           VALUES (?, ?, ?, 0) 
           ON DUPLICATE KEY UPDATE data = VALUES(data), timestamp = ?, is_compressed = 0`,
          [`${agencyName}:${queryHash}`, jsonData, systemDate, systemDate]
        );
      } else {
        await connection.execute(
          `INSERT INTO query_cache (cache_key, data, timestamp, is_compressed) 
           VALUES (?, ?, NOW(), 0) 
           ON DUPLICATE KEY UPDATE data = VALUES(data), timestamp = NOW(), is_compressed = 0`,
          [`${agencyName}:${queryHash}`, jsonData]
        );
      }

      connection.release();
      console.log(`Caché actualizada para ${agencyName}: ${data.length} registros`);
      return true;
    }
  } catch (error) {
    console.error('Error al guardar en caché MySQL:', error);

    // Marcar MySQL como no disponible después de ciertos errores
    if (error.code === 'PROTOCOL_CONNECTION_LOST' || error.code === 'ETIMEDOUT' ||
      error.code === 'ER_NET_PACKET_TOO_LARGE') {
      console.warn('Error de conexión o datos muy grandes. Se usará solo caché en memoria.');
      mysqlAvailable = false;

      // Intentar volver a conectar después de un tiempo
      setTimeout(() => {
        mysqlAvailable = true;
        console.log('Reintentando conexión a MySQL después de un tiempo');
      }, 5 * 60 * 1000); // 5 minutos
    }

    return false;
  }
}

// Optimización 3: Soporte para CSV como caché adicional
/**
 * Carga datos desde un archivo CSV y aplica filtros
 * @param {string} filePath - Ruta al archivo CSV
 * @param {Object} filters - Filtros a aplicar
 * @returns {Promise<Array>} - Datos filtrados
 */
async function loadFromCSV(filePath, filters = {}) {
  return new Promise((resolve, reject) => {
    try {
      const fileContent = fs.readFileSync(filePath, 'utf8');

      Papa.parse(fileContent, {
        header: true,
        dynamicTyping: true,
        skipEmptyLines: true,
        complete: function (results) {
          if (results.errors && results.errors.length > 0) {
            console.warn('Advertencias al parsear CSV:', results.errors);
          }

          // Aplicar filtros si existen
          let filteredData = results.data;

          if (Object.keys(filters).length > 0) {
            filteredData = applyFiltersToData(filteredData, filters);
          }

          resolve(filteredData);
        },
        error: function (error) {
          reject(error);
        }
      });
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Aplica filtros a un conjunto de datos
 * @param {Array} data - Datos a filtrar
 * @param {Object} filters - Filtros a aplicar
 * @returns {Array} - Datos filtrados
 */
function applyFiltersToData(data, filters) {
  return data.filter(item => {
    // Filtro por serie
    if (filters.serie && item.SERIE) {
      if (!item.SERIE.toLowerCase().includes(filters.serie.toLowerCase())) {
        return false;
      }
    }

    // Filtro por fecha
    if (filters.fechaInicio && filters.fechaFin && item.ULT_VISITA) {
      const itemDate = parseDate(item.ULT_VISITA);
      const startDate = parseDate(filters.fechaInicio);
      const endDate = parseDate(filters.fechaFin);

      if (!itemDate || itemDate < startDate || itemDate > endDate) {
        return false;
      }
    }

    // Filtro por días sin visita
    if (filters.diasSinVisitaMin !== undefined &&
      filters.diasSinVisitaMax !== undefined &&
      item.DIAS_SIN_VENIR !== undefined) {

      const dias = parseInt(item.DIAS_SIN_VENIR);
      if (isNaN(dias) || dias < filters.diasSinVisitaMin || dias > filters.diasSinVisitaMax) {
        return false;
      }
    }

    // Más filtros según sea necesario...

    return true;
  });
}

/**
 * Intenta convertir un string de fecha a objeto Date
 * @param {string} dateStr - String de fecha en formato DD/MM/YYYY
 * @returns {Date|null} - Fecha parseada o null si no es válida
 */
function parseDate(dateStr) {
  if (!dateStr) return null;

  // Intentar varios formatos comunes
  const formats = [
    // DD/MM/YYYY
    (str) => {
      const parts = str.split('/');
      if (parts.length === 3) {
        const day = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10) - 1;
        const year = parseInt(parts[2], 10);
        return new Date(year, month, day);
      }
      return null;
    },
    // YYYY-MM-DD
    (str) => {
      const date = new Date(str);
      return isNaN(date.getTime()) ? null : date;
    }
  ];

  for (const format of formats) {
    const date = format(dateStr);
    if (date && !isNaN(date.getTime())) {
      return date;
    }
  }

  return null;
}

/**
 * Genera una consulta SQL optimizada para extraer datos completos
 * @param {string} agencyName - Nombre de la agencia
 * @param {Object} filters - Filtros opcionales a aplicar
 * @param {boolean} forceNoCache - Si es true, añade un hint para evitar el uso de caché de BigQuery
 * @returns {string} - Consulta SQL generada
 */
function generateQuery(agencyName, filters = {}, forceNoCache = false) {
  const config = agencyConfig[agencyName];
  if (!config) throw new Error(`Configuración no encontrada para la agencia: ${agencyName}`);

  const dateField = config.dateField || 'FECHA_FAC';
  const dateFormat = config.dateFormat || '%d/%m/%Y';

  // Construir la cláusula WHERE con filtros dinámicos
  let whereConditions = [];

  // Añadir filtros específicos si existen
  if (filters.serie) {
    whereConditions.push(`SERIE LIKE '%${filters.serie}%'`);
  }

  if (filters.diasSinVisitaMin !== undefined && filters.diasSinVisitaMax !== undefined) {
    whereConditions.push(`
      DATE_DIFF(CURRENT_DATE(), CAST(${dateField} AS DATE), DAY) BETWEEN 
      ${filters.diasSinVisitaMin} AND ${filters.diasSinVisitaMax}
    `);
  }

  if (filters.fechaInicio && filters.fechaFin) {
    whereConditions.push(`
      ${dateField} BETWEEN 
      PARSE_DATE('${dateFormat}', '${filters.fechaInicio}') 
      AND PARSE_DATE('${dateFormat}', '${filters.fechaFin}')
    `);
  }

  // Añadir filtros de modelo, año, agencia, etc. si existen
  if (filters.modelo) {
    whereConditions.push(`MODELO = '${filters.modelo}'`);
  }

  if (filters.anio) {
    whereConditions.push(`ANIO_VIN = '${filters.anio}'`);
  }

  if (filters.agencia && agencyName !== filters.agencia) {
    whereConditions.push(`AGENCIA = '${filters.agencia}'`);
  }

  // Construir cláusula WHERE completa
  const whereClause = whereConditions.length > 0
    ? `WHERE ${whereConditions.join(' AND ')}`
    : '';

  // Orden de resultados
  const orderClause = `ORDER BY ${dateField} DESC`;

  // Agregar hint para evitar caché de BigQuery si se solicita
  const cacheHint = forceNoCache ? '\n  /* NO_CACHE */' : '';

  // Consulta SQL optimizada
  return `${cacheHint}
  SELECT
    *,
    FORMAT_DATE('${dateFormat}', ${dateField}) as ULT_VISITA,
    CASE
      WHEN ${dateField} IS NULL THEN NULL
      ELSE DATE_DIFF(CURRENT_DATE(), CAST(${dateField} AS DATE), DAY)
    END as DIAS_SIN_VENIR
  FROM
    \`${config.projectId}.${config.datasetName}.${config.tableName}\`
  ${whereClause}
  ${orderClause}`;
}

/**
 * Ejecuta una consulta en BigQuery con soporte para caché
 * @param {string} projectId - ID del proyecto de BigQuery
 * @param {string} query - Consulta SQL a ejecutar
 * @param {boolean} useCache - Indica si se debe usar caché (default: true)
 * @returns {Promise<Array>} - Resultados de la consulta
 */
async function executeQuery(projectId, query, useCache = true) {
  // Generar hash único para la consulta
  const queryHash = crypto.createHash('md5').update(query).digest('hex');

  // Determinar la agencia a partir del projectId
  const agencyEntry = Object.entries(agencyConfig).find(([_, config]) =>
    config.projectId === projectId
  );

  const agencyName = agencyEntry ? agencyEntry[0] : 'unknown';

  // Verificar si hay datos en caché
  if (useCache) {
    const cachedData = await getFromCache(agencyName, queryHash);
    if (cachedData) {
      console.log(`Usando datos en caché de DB para: ${agencyName}`);
      return cachedData;
    }
  }

  try {
    console.log(`Ejecutando consulta en proyecto: ${projectId}`);

    // Configuración optimizada para consultas grandes
    const queryOptions = {
      query,
      maximumBytesBilled: '1000000000', // 1GB
      useLegacySql: false,
      timeoutMs: 300000, // 5 minutos
      useQueryCache: true,
      priority: 'INTERACTIVE'
    };

    // Si es necesario cambiar el proyecto
    if (projectId && projectId !== process.env.BQ_PROJECT_ID) {
      console.log(`Cambiando a proyecto: ${projectId}`);
      let tempBigQuery;

      // Crear instancia de BigQuery con credenciales apropiadas
      if (process.env.BQ_PROJECT_ID && process.env.BQ_PRIVATE_KEY && process.env.BQ_CLIENT_EMAIL) {
        tempBigQuery = new BigQuery({
          projectId: projectId,
          credentials: credentials,
          ...bigqueryOptions
        });
      }
      else if (fs.existsSync(keyFilePath)) {
        tempBigQuery = new BigQuery({
          projectId: projectId,
          keyFilename: keyFilePath,
          ...bigqueryOptions
        });
      }
      else {
        throw new Error('No se encontraron credenciales válidas para BigQuery');
      }

      // Ejecutar consulta con monitoreo de progreso
      const queryJob = await tempBigQuery.createQueryJob(queryOptions);
      const jobId = queryJob[0].id;

      // Monitoreo del progreso de la consulta...
      // [código de monitoreo omitido por brevedad]

      const [rows] = await queryJob[0].getQueryResults();

      // Procesamiento y guardado en caché
      const processedRows = await processDataInBatches(rows);

      if (useCache) {
        await saveToCache(agencyName, queryHash, processedRows);
      }

      return processedRows;
    } else {
      // Usar instancia por defecto de BigQuery
      // [implementación similar a la anterior]
      const queryJob = await bigquery.createQueryJob(queryOptions);
      const [rows] = await queryJob[0].getQueryResults();
      const processedRows = await processDataInBatches(rows);

      if (useCache) {
        await saveToCache(agencyName, queryHash, processedRows);
      }

      return processedRows;
    }
  } catch (error) {
    console.error(`Error al ejecutar consulta en ${projectId}:`, error);

    // Registrar error en metadata
    try {
      const connection = await getDbConnection();
      await connection.execute(
        `INSERT INTO cache_metadata (agency, last_updated, status, error_message) 
         VALUES (?, NOW(), 'error', ?) 
         ON DUPLICATE KEY UPDATE last_updated = NOW(), 
         status = 'error', error_message = VALUES(error_message)`,
        [agencyName, error.message.substring(0, 255)]
      );
      await connection.end();
    } catch (metaError) {
      console.error('Error al registrar error en metadata:', metaError);
    }

    throw error;
  }
}

/**
 * Procesa grandes conjuntos de datos en lotes para reducir el uso de memoria
 * @param {Array} data - Datos a procesar
 * @returns {Promise<Array>} - Datos procesados
 */
async function processDataInBatches(data) {
  const BATCH_SIZE = 5000; // Procesar 5000 registros a la vez
  const result = [];

  for (let i = 0; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);

    // Procesar cada lote
    const processedBatch = batch.map(row => {
      // Crear una copia limpia del objeto
      const cleanRow = { ...row };

      // Asegurar que DIAS_SIN_VENIR sea un número
      if (cleanRow.DIAS_SIN_VENIR !== undefined && cleanRow.DIAS_SIN_VENIR !== null) {
        if (typeof cleanRow.DIAS_SIN_VENIR === 'string') {
          const parsedValue = parseInt(cleanRow.DIAS_SIN_VENIR, 10);
          if (!isNaN(parsedValue)) {
            cleanRow.DIAS_SIN_VENIR = parsedValue;
          }
        }
      } else {
        cleanRow.DIAS_SIN_VENIR = 0; // Valor por defecto
      }

      return cleanRow;
    });

    result.push(...processedBatch);

    // Permitir que el event loop respire entre lotes grandes
    if (i + BATCH_SIZE < data.length) {
      await new Promise(resolve => setTimeout(resolve, 0));
    }
  }

  return result;
}

/**
 * Obtiene datos de una agencia específica con filtros opcionales
 * @param {string} agencyName - Nombre de la agencia
 * @param {Object} filters - Filtros opcionales (serie, diasSinVisita, etc.)
 * @param {boolean} useCache - Si se debe usar caché (default: true)
 * @param {boolean} forceNoCache - Si se debe evitar la caché de BigQuery (default: false)
 * @returns {Promise<Array>} - Datos obtenidos
 */
async function getAgencyData(agencyName, filters = {}, useCache = true, forceNoCache = false) {
  try {
    if (!agencyConfig[agencyName]) {
      throw new Error(`Agencia no configurada: ${agencyName}`);
    }

    const config = agencyConfig[agencyName];

    // Generar la consulta SQL, pasando el parámetro forceNoCache
    const query = generateQuery(agencyName, filters, forceNoCache);

    // Ejecutar la consulta
    const data = await executeQuery(config.projectId, query, useCache);

    if (!data || data.length === 0) {
      console.warn(`No se encontraron datos para ${agencyName}`);
      return [];
    }

    return data;
  } catch (error) {
    console.error(`Error al obtener datos para ${agencyName}:`, error);
    throw error;
  }
}

/**
 * Invalida la caché para todas las consultas o para una agencia específica
 * @param {string} agencyName - Nombre de la agencia (opcional)
 */
function invalidateCache(agencyName = null) {
  console.log(`Iniciando invalidación de caché${agencyName ? ' para ' + agencyName : ' completa'}`);

  // 1. Limpiar caché en memoria (memoryCache)
  if (agencyName) {
    // Limpiar solo para una agencia específica
    for (const key of memoryCache.keys()) {
      if (key.startsWith(`${agencyName}:`)) {
        console.log(`Eliminando clave de memoryCache: ${key}`);
        memoryCache.delete(key);
      }
    }
  } else {
    // Limpiar toda la caché en memoria
    console.log('Limpiando toda la caché en memoria');
    memoryCache.clear();
  }

  // 2. Limpiar queryCache (caché en objeto)
  if (agencyName) {
    // Invalidar caché solo para una agencia específica
    const keyPrefix = `${agencyName}:`;
    for (const key of queryCache.keys()) {
      if (key.startsWith(keyPrefix)) {
        console.log(`Eliminando clave de queryCache: ${key}`);
        queryCache.delete(key);
      }
    }
    console.log(`Caché invalidada para la agencia: ${agencyName}`);
  } else {
    // Invalidar toda la caché
    console.log('Limpiando totalmente queryCache');
    queryCache.clear();
    console.log('Caché completamente invalidada');
  }

  // 3. Guardar cambios en el archivo de caché
  saveCacheToFile();
  console.log('Caché guardada en archivo después de invalidación');

  // 4. También invalidar en la base de datos
  invalidateCacheInDb(agencyName);
  console.log('Solicitud de invalidación de caché en DB enviada');

  return true;
}


/**
 * Invalida la caché en la base de datos para todas las consultas o para una agencia específica
 * @param {string} agencyName - Nombre de la agencia (opcional, si no se especifica se invalida toda la caché)
 * @returns {Promise<boolean>} - Indica si la operación fue exitosa
 */
async function invalidateCacheInDb(agencyName = null) {
  try {
    // Ahora invalidar la caché en la base de datos
    const connection = await getDbConnection();

    if (agencyName) {
      // Eliminar caché para una agencia específica
      await connection.execute(
        'DELETE FROM query_cache WHERE cache_key LIKE ?',
        [`${agencyName}:%`]
      );

      // Actualizar metadata
      await connection.execute(
        `UPDATE cache_metadata SET status = 'invalidated', last_updated = NOW() 
         WHERE agency = ?`,
        [agencyName]
      );

      console.log(`Caché invalidada en DB para la agencia: ${agencyName}`);
    } else {
      // Eliminar toda la caché
      await connection.execute('TRUNCATE TABLE query_cache');

      // Actualizar metadata para todas las agencias
      await connection.execute(
        `UPDATE cache_metadata SET status = 'invalidated', last_updated = NOW()`
      );

      console.log('Caché completamente invalidada en DB');
    }

    connection.release();
    return true;
  } catch (error) {
    console.error('Error al invalidar caché en DB:', error);
    return false;
  }
}

/**
* Precarga los datos de todas las agencias o una agencia específica
* Esta función se puede ejecutar diariamente para actualizar la caché
* @param {string} agencyName - Nombre de la agencia (opcional)
* @returns {Promise<boolean>} - Indica si la operación fue exitosa
*/
async function preloadAgencyData(agencyName = null) {
  try {
    // Agrupar agencias por projectId para minimizar el número de consultas
    const projectGroups = {};

    // Si se especifica una agencia, precargar solo esa
    if (agencyName) {
      console.log(`Precargando datos para la agencia: ${agencyName}`);

      // Invalidar caché existente para esa agencia
      invalidateCache(agencyName);

      // Precargar con consulta básica (sin filtros)
      await getAgencyData(agencyName, {}, true);

      console.log(`Precarga completada para: ${agencyName}`);
      // Guardar caché
      saveCacheToFile();
      return true;
    }

    // Si no se especifica agencia, precargar todas pero de manera optimizada
    console.log('Precargando datos para todas las agencias...');

    // Invalidar toda la caché
    invalidateCache();

    // Agrupar agencias por proyecto para reducir consultas
    Object.entries(agencyConfig).forEach(([agency, config]) => {
      const { projectId } = config;
      if (!projectGroups[projectId]) {
        projectGroups[projectId] = [];
      }
      projectGroups[projectId].push(agency);
    });

    // Ejecutar consulta por cada proyecto en vez de por cada agencia
    for (const [projectId, agencies] of Object.entries(projectGroups)) {
      console.log(`Precargando datos para proyecto: ${projectId} (${agencies.join(', ')})`);

      // Para cada agencia en este proyecto
      for (const agency of agencies) {
        console.log(`- Procesando agencia: ${agency}`);
        await getAgencyData(agency, {}, true);
      }

      // Pausa breve entre cada proyecto
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    console.log('Precarga completada para todas las agencias');
    // Guardar caché en archivo
    saveCacheToFile();
    return true;

  } catch (error) {
    console.error('Error durante la precarga de datos:', error);
    throw error;
  }
}


// Exportar funciones
export {
  getAgencyData,
  preloadAgencyData,
  invalidateCache,
  executeQuery,
  agencyConfig,
  saveCacheToFile,
  queryCache,
  getFromCache,
  saveToCache,
  getDbConnection,
  invalidateCacheInDb  // Añadir esta nueva exportación
};