import { BigQuery } from '@google-cloud/bigquery';
import 'dotenv/config';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24 horas en milisegundos
const CACHE_DIR = path.join(__dirname, '../../../cache');
const CACHE_FILENAME = path.join(CACHE_DIR, 'bigquery_cache.json');

// Asegurar que existe el directorio de caché
if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
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

// Instanciar el cliente de BigQuery con manejo de errores
const keyFilePath = path.join(__dirname, '../../google-credentials.json');
console.log(`Buscando archivo de credenciales en: ${keyFilePath}`);


let bigquery;
try {
  // Verifica si existe el archivo de credenciales
  if (!fs.existsSync(keyFilePath)) {
    throw new Error(`No se encontró el archivo de credenciales en: ${keyFilePath}`);
  }

  // Inicializa BigQuery con el archivo de credenciales
  bigquery = new BigQuery({
    projectId: process.env.BQ_PROJECT_ID || JSON.parse(fs.readFileSync(keyFilePath, 'utf8')).project_id,
    keyFilename: keyFilePath
  });
  console.log('✅ Cliente BigQuery inicializado correctamente');
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
  },
  'Gasme': {
    projectId: 'base-maestra-gn',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    dateField: 'FECHA_FAC',
    dateFormat: '%d/%m/%Y'
  }
};

/**
 * Genera una consulta SQL para extraer datos de la tabla de retención
 * @param {string} agencyName - Nombre de la agencia
 * @param {Object} filters - Filtros opcionales a aplicar (opcional)
 * @returns {string} - Consulta SQL generada
 */
function generateQuery(agencyName, filters = {}) {
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

  // Construir cláusula WHERE completa
  const whereClause = whereConditions.length > 0
    ? `WHERE ${whereConditions.join(' AND ')}`
    : '';

  // Límite de registros si se especifica
  const limitClause = filters.limit ? `LIMIT ${filters.limit}` : '';

  // Orden de resultados
  const orderClause = `ORDER BY ${dateField} DESC`;

  // Consulta SQL completa
  return `
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
  ${orderClause}
  ${limitClause}`;
}

/**
 * Ejecuta una consulta en BigQuery con soporte para caché
 * @param {string} projectId - ID del proyecto de BigQuery
 * @param {string} query - Consulta SQL a ejecutar
 * @param {boolean} useCache - Indica si se debe usar caché (default: true)
 * @returns {Promise<Array>} - Resultados de la consulta
 */
async function executeQuery(projectId, query, useCache = true) {
  // Generar clave de caché basada en la query
  const cacheKey = `${projectId}:${query}`;

  // Verificar si hay datos en caché y si no han expirado
  if (useCache && queryCache.has(cacheKey)) {
    const cachedData = queryCache.get(cacheKey);
    const now = Date.now();

    if (now - cachedData.timestamp < CACHE_DURATION) {
      console.log(`Usando datos en caché para: ${projectId}`);
      return cachedData.data;
    }
  }

  try {
    console.log(`Ejecutando consulta en proyecto: ${projectId}`);
    console.log(`Consulta a ejecutar:\n${query}`);

    // Si es necesario cambiar el proyecto y es diferente al predeterminado
    if (projectId && projectId !== (process.env.BQ_PROJECT_ID || JSON.parse(fs.readFileSync(keyFilePath, 'utf8')).project_id)) {
      console.log(`Cambiando a proyecto: ${projectId}`);

      const tempBigQuery = new BigQuery({
        projectId: projectId,
        keyFilename: keyFilePath
      });

      const [rows] = await tempBigQuery.query({ query });
      console.log(`Consulta exitosa en ${projectId}: ${rows.length} filas obtenidas`);

      // Guardar en caché si está habilitada
      if (useCache) {
        queryCache.set(cacheKey, {
          data: rows,
          timestamp: Date.now()
        });
        saveCacheToFile();
      }

      return rows;
    } else {
      // Usar la instancia por defecto
      console.log('Usando instancia por defecto de BigQuery');
      const [rows] = await bigquery.query({ query });
      console.log(`Consulta exitosa en ${projectId}: ${rows.length} filas obtenidas`);

      // Guardar en caché si está habilitada
      if (useCache) {
        queryCache.set(cacheKey, {
          data: rows,
          timestamp: Date.now()
        });
        saveCacheToFile();
      }

      return rows;
    }
  } catch (error) {
    console.error(`Error al ejecutar consulta en ${projectId}:`, error);

    // Proporcionar información más detallada sobre el error
    if (error.code === 403) {
      console.error(`⚠️ Error de permisos: Verifica que la cuenta de servicio tiene los permisos necesarios en el proyecto ${projectId}`);
    } else if (error.code === 400) {
      console.error('⚠️ Error en la consulta SQL: Verifica los nombres de las columnas y la sintaxis');

      // Extraer el mensaje de error específico si está disponible
      if (error.errors && error.errors.length > 0) {
        console.error(`Detalles del error: ${error.errors[0].message}`);
      }
    }

    throw error;
  }
}

/**
 * Obtiene datos de una agencia específica con filtros opcionales
 * @param {string} agencyName - Nombre de la agencia
 * @param {Object} filters - Filtros opcionales (serie, diasSinVisita, etc.)
 * @param {boolean} useCache - Si se debe usar caché (default: true)
 * @returns {Promise<Array>} - Datos obtenidos
 */
async function getAgencyData(agencyName, filters = {}, useCache = true) {
  try {
    if (!agencyConfig[agencyName]) {
      throw new Error(`Agencia no configurada: ${agencyName}`);
    }

    const config = agencyConfig[agencyName];

    // Generar la consulta SQL
    const query = generateQuery(agencyName, filters);

    // Ejecutar la consulta
    const data = await executeQuery(config.projectId, query, useCache);

    if (!data || data.length === 0) {
      console.warn(`No se encontraron datos para ${agencyName}`);
      return [];
    }

    // Procesamiento de datos para estandarizar
    return data.map(row => {
      // Crear una copia limpia y segura del objeto
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
  } catch (error) {
    console.error(`Error al obtener datos para ${agencyName}:`, error);
    throw error;
  }
}

/**
 * Invalida la caché para todas las consultas o para una agencia específica
 * @param {string} agencyName - Nombre de la agencia (opcional, si no se especifica se invalida toda la caché)
 */
function invalidateCache(agencyName = null) {
  if (agencyName) {
    // Invalidar caché solo para una agencia específica
    const keyPrefix = `${agencyConfig[agencyName]?.projectId}:`;
    for (const key of queryCache.keys()) {
      if (key.startsWith(keyPrefix)) {
        queryCache.delete(key);
      }
    }
    console.log(`Caché invalidada para la agencia: ${agencyName}`);
  } else {
    // Invalidar toda la caché
    queryCache.clear();
    console.log('Caché completamente invalidada');
  }

  // Guardar cambios en el archivo
  saveCacheToFile();
}

/**
 * Precarga los datos de todas las agencias o una agencia específica
 * Esta función se puede ejecutar diariamente para actualizar la caché
 * @param {string} agencyName - Nombre de la agencia (opcional, si no se especifica se precargan todas)
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
// Exportar funciones
export {
  getAgencyData,
  preloadAgencyData,
  invalidateCache,
  executeQuery,
  agencyConfig,
  saveCacheToFile,
  queryCache
};