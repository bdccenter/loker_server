// server/index.js

// Importaciones existentes
import express from 'express';
import cors from 'cors';
import crypto from 'crypto';
import mysql from 'mysql2/promise';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';
import { initializeDatabase } from './service/dbInitService.js';

// Nuevas importaciones
import compression from 'compression';
import zlib from 'zlib';

// Cargar variables de entorno
dotenv.config();

// Importar servicios BigQuery
import { getAgencyData, invalidateCache, preloadAgencyData, agencyConfig, queryCache, getFromCache } from './service/bigQueryDirectService.js';

// Obtener la ruta del archivo actual (necesario en ES modules)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Definir rutas para logs y cache
const LOGS_DIR = path.join(__dirname, '../../logs');
const CACHE_DIR = path.join(__dirname, '../../cache');

// Asegurar que existen los directorios
if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR, { recursive: true });
}
if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

// Crear la aplicación Express
const app = express();
const port = process.env.PORT || 3001;

// Aplicar middleware de compresión para todas las respuestas
app.use(compression({
  level: zlib.constants.Z_BEST_COMPRESSION,
  threshold: 0 // Comprimir todas las respuestas sin importar el tamaño
}));

// Configuración CORS básica
app.use(cors());

// Middleware para parsear JSON
app.use(express.json());

try { // Incializador de base de datos
  console.log("Inicializando tablas de base de datos...");
  await initializeDatabase(); 
  console.log("Base de datos inicializada correctamente");
} catch (error) {
  console.error("Error al inicializar la base de datos:", error);
}

// Configuración de la conexión a la base de datos MySQL
const dbConfig = {
  host: process.env.HOST_DB || 'localhost',
  port: process.env.PORT_DB || 3306,
  user: process.env.USER || 'root',
  password: process.env.PASSWORD || 'root',
  database: process.env.DATABASE || 'test',
  connectionLimit: process.env.CONNECTION_LIMIT || 10,
};

// Crear el pool de conexiones
const pool = mysql.createPool(dbConfig);

// Función para hashear contraseñas
const hashPassword = (password) => {
  return crypto.createHash('sha256').update(password).digest('hex');
};

// Ruta para verificar que el servidor está funcionando
app.get('/api/test', (req, res) => {
  res.json({ message: 'Servidor funcionando correctamente' });
});

// Endpoint para obtener datos de una agencia específica con caché HTTP
app.get('/api/data/:agencyName', async (req, res) => {
  try {
    const { agencyName } = req.params;
    const filters = req.query;
    console.log(`Solicitud de datos para agencia: ${agencyName}, filtros:`, filters);

    // Configurar cabeceras de caché HTTP
    res.setHeader('Cache-Control', 'public, max-age=300'); // 5 minutos
    
    // Generar un ETag basado en la agencia y los filtros
    const etagBase = JSON.stringify({ agencyName, filters });
    const etag = `"${crypto.createHash('md5').update(etagBase).digest('hex')}"`;
    res.setHeader('ETag', etag);
    
    // Comprobar si podemos devolver 304 Not Modified
    const ifNoneMatch = req.headers['if-none-match'];
    if (ifNoneMatch && ifNoneMatch === etag) {
      return res.status(304).end();
    }

    // Forzar el uso de caché siempre que sea posible
    const useCache = filters.useCache !== 'false';

    const data = await getAgencyData(agencyName, filters, useCache);
    console.log(`Datos obtenidos para ${agencyName}: ${data.length} registros`);

    res.json(data);
  } catch (error) {
    console.error('Error en endpoint /api/data:', error);
    res.status(500).json({ error: error.message });
  }
});

// Nuevo endpoint para datos paginados
app.get('/api/data/:agencyName/paginated', async (req, res) => {
  try {
    const { agencyName } = req.params;
    const start = parseInt(req.query.start || '0', 10);
    const limit = parseInt(req.query.limit || '100', 10);
    const useCache = req.query.useCache !== 'false';
    
    console.log(`Solicitud de datos paginados para agencia: ${agencyName}, inicio: ${start}, límite: ${limit}`);
    
    // Configurar cabeceras de caché HTTP
    res.setHeader('Cache-Control', 'public, max-age=300'); // 5 minutos
    
    // Extraer filtros de los query params
    const filters = { ...req.query };
    delete filters.start;
    delete filters.limit;
    delete filters.useCache;
    
    // Intentar obtener datos completos desde la caché
    let allData;
    
    try {
      // Generar un hash para la consulta sin paginación
      const baseQueryHash = crypto.createHash('md5').update(JSON.stringify(filters)).digest('hex');
      const allDataCacheKey = `${agencyName}:${baseQueryHash}`;
      
      // Buscar en caché primero
      allData = await getFromCache(agencyName, baseQueryHash);
      
      if (!allData) {
        // Si no está en caché, obtener datos completos
        allData = await getAgencyData(agencyName, filters, useCache);
      }
    } catch (cacheError) {
      console.error('Error al obtener datos completos:', cacheError);
      
      // Si falla la caché, cargar los datos normalmente
      allData = await getAgencyData(agencyName, filters, useCache);
    }
    
    // Si no hay datos, devolver array vacío
    if (!allData || allData.length === 0) {
      return res.json({
        data: [],
        total: 0,
        page: 1,
        pageSize: limit,
        totalPages: 0
      });
    }
    
    // Calcular información de paginación
    const totalItems = allData.length;
    const totalPages = Math.ceil(totalItems / limit);
    const currentPage = Math.floor(start / limit) + 1;
    
    // Obtener solo la porción solicitada
    const paginatedData = allData.slice(start, start + limit);
    
    // Enviar respuesta con metadatos de paginación
    res.json({
      data: paginatedData,
      total: totalItems,
      page: currentPage,
      pageSize: limit,
      totalPages: totalPages
    });
  } catch (error) {
    console.error('Error en endpoint de datos paginados:', error);
    res.status(500).json({ error: error.message });
  }
});

// Agregar el resto de endpoints de BigQuery
app.post('/api/cache/invalidate/:agencyName', (req, res) => {
  try { // Endpoint para invalidar caché de una agencia específica
    const { agencyName } = req.params;
    console.log(`Invalidando caché para agencia: ${agencyName}`);

    invalidateCache(agencyName);

    res.json({ success: true, message: `Caché invalidada para ${agencyName}` });
  } catch (error) { // Manejo de errores
    console.error('Error en endpoint /api/cache/invalidate:', error);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint para actualizar manualmente los datos
app.post('/api/update', async (req, res) => {
  try { // Endpoint para actualizar manualmente los datos
    console.log("Iniciando actualización manual de datos...");
    const { performUpdate } = await import('./service/scheduleDataUpdates.js');

    const success = await performUpdate('manual');

    res.json({
      success,
      message: success
        ? "Actualización de datos completada exitosamente"
        : "Actualización completada con advertencias"
    });
  } catch (error) {
    console.error("Error durante la actualización manual:", error);
    res.status(500).json({
      success: false,
      error: error.message,
      message: "Error al realizar la actualización de datos"
    });
  }
});

// GET - Obtener todos los usuarios
app.get('/users', async (req, res) => {
  let connection;
  try {  // endopoint para obtener todos los usuarios
    // Obtener una conexión del pool
    connection = await pool.getConnection();

    // Consultar los usuarios
    const [rows] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users'
    );
    // Devolver los usuarios en formato JSON
    res.json({ users: rows });
  } catch (error) {  // Manejo de errores
    console.error('Error al obtener usuarios:', error);
    res.status(500).json({ message: 'Error al obtener usuarios' });
  } finally { // liberar la conexion
    if (connection) connection.release();
  }
});

// GET - Obtener un usuario específico
app.get('/users/:id', async (req, res) => {
  const userId = parseInt(req.params.id);
  let connection;

  try { // endpoint para obtener un usuario en específico
    connection = await pool.getConnection();

    const [rows] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users WHERE id = ?',
      [userId]
    );

    if (rows.length === 0) { // Si no se encuentra el usuario, devolver un error 404
      return res.status(404).json({ message: 'Usuario no encontrado' });
    }

    res.json({ user: rows[0] });
  } catch (error) {
    console.error(`Error al obtener usuario ${userId}:`, error);
    res.status(500).json({ message: 'Error al obtener usuario' });
  } finally {
    if (connection) connection.release();
  }
});

// POST - Crear un nuevo usuario
app.post('/users', async (req, res) => {
  const { password, is_superuser, first_name, last_name, email, Agencia } = req.body;
  let connection;

  // Validar campos requeridos
  if (!password || !first_name || !last_name || !email) {
    return res.status(400).json({ message: 'Todos los campos son requeridos excepto Agencia' });
  }

  try {
    connection = await pool.getConnection();

    // Verificar si el email ya existe
    const [existingUsers] = await connection.execute(
      'SELECT id FROM users WHERE email = ?',
      [email]
    );

    if (existingUsers.length > 0) {
      return res.status(400).json({ message: 'Ya existe un usuario con ese email' });
    }

    // Hashear la contraseña
    const hashedPassword = hashPassword(password);

    // Insertar nuevo usuario (sin especificar el ID, dejando que AUTO_INCREMENT lo maneje)
    const [result] = await connection.execute(
      'INSERT INTO users (password, is_superuser, first_name, last_name, email, Agencia) VALUES (?, ?, ?, ?, ?, ?)',
      [hashedPassword, is_superuser ? 1 : 0, first_name, last_name, email, Agencia || null]
    );

    // Obtener el ID del usuario recién creado
    const insertId = result.insertId;

    // Obtener el usuario recién creado
    const [newUser] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users WHERE id = ?',
      [insertId]
    );

    res.status(201).json({ message: 'Usuario creado con éxito', user: newUser[0] });
  } catch (error) {
    console.error('Error al crear usuario:', error);
    res.status(500).json({ message: 'Error al crear usuario' });
  } finally {
    if (connection) connection.release();
  }
});

// PUT - Actualizar un usuario existente
app.put('/users/:id', async (req, res) => {
  const userId = parseInt(req.params.id);
  const { password, is_superuser, first_name, last_name, email, Agencia } = req.body;
  let connection;

  try {
    connection = await pool.getConnection();

    // Verificar si el usuario existe
    const [existingUser] = await connection.execute(
      'SELECT id FROM users WHERE id = ?',
      [userId]
    );

    if (existingUser.length === 0) {
      return res.status(404).json({ message: 'Usuario no encontrado' });
    }

    // Verificar si el email ya está en uso por otro usuario
    if (email) {
      const [emailExists] = await connection.execute(
        'SELECT id FROM users WHERE email = ? AND id != ?',
        [email, userId]
      );

      if (emailExists.length > 0) {
        return res.status(400).json({ message: 'El email ya está en uso por otro usuario' });
      }
    }

    // Construir la consulta de actualización dinámicamente
    let updateQuery = 'UPDATE users SET ';
    const updateValues = [];

    if (first_name) {
      updateQuery += 'first_name = ?, ';
      updateValues.push(first_name);
    }

    if (last_name) {
      updateQuery += 'last_name = ?, ';
      updateValues.push(last_name);
    }

    if (email) {
      updateQuery += 'email = ?, ';
      updateValues.push(email);
    }

    // Solo actualizar la contraseña si se proporciona una nueva
    if (password) {
      updateQuery += 'password = ?, ';
      updateValues.push(hashPassword(password));
    }

    // Siempre actualizar is_superuser y Agencia
    updateQuery += 'is_superuser = ?, Agencia = ? ';
    updateValues.push(is_superuser ? 1 : 0);
    updateValues.push(Agencia === '' ? null : Agencia);

    updateQuery += 'WHERE id = ?';
    updateValues.push(userId);

    // Ejecutar la actualización
    await connection.execute(updateQuery, updateValues);

    // Obtener el usuario actualizado
    const [updatedUser] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users WHERE id = ?',
      [userId]
    );

    res.json({ message: 'Usuario actualizado con éxito', user: updatedUser[0] });
  } catch (error) {
    console.error(`Error al actualizar usuario ${userId}:`, error);
    res.status(500).json({ message: 'Error al actualizar usuario' });
  } finally {
    if (connection) connection.release();
  }
});

// DELETE - Eliminar un usuario
app.delete('/users/:id', async (req, res) => {
  const userId = parseInt(req.params.id);
  let connection;

  try {
    // Obtener una conexión del pool
    connection = await pool.getConnection();

    // Verificar si el usuario existe
    const [existingUser] = await connection.execute(
      'SELECT id FROM users WHERE id = ?',
      [userId]
    );

    // Si no existe, devolver un error 404
    if (existingUser.length === 0) {
      return res.status(404).json({ message: 'Usuario no encontrado' });
    }

    // Eliminar el usuario
    await connection.execute(
      'DELETE FROM users WHERE id = ?',
      [userId]
    );

    res.json({ message: 'Usuario eliminado con éxito', success: true });
  } catch (error) {
    console.error(`Error al eliminar usuario ${userId}:`, error);
    res.status(500).json({ message: 'Error al eliminar usuario' });
  } finally {
    if (connection) connection.release();
  }
});

// Endpoint para login
app.post('/login', async (req, res) => {
  const { email, password } = req.body;
  let connection;

  // Validar que se proporcionaron email y password
  if (!email || !password) {
    return res.status(400).json({ message: 'Se requiere email y contraseña' });
  }

  try {
    // Obtener una conexión del pool
    connection = await pool.getConnection();

    // Buscar al usuario por su email
    const [rows] = await connection.execute(
      'SELECT id, password, is_superuser, first_name, last_name, email, Agencia FROM users WHERE email = ? LIMIT 1',
      [email]
    );

    // Verificar si el usuario existe
    if (rows.length === 0) {
      return res.status(401).json({ message: 'Credenciales inválidas' });
    }

    const user = rows[0];

    // Verificar la contraseña
    const hash = crypto.createHash('sha256').update(password).digest('hex');
    const passwordValid = hash === user.password || password === user.password;

    if (!passwordValid) {
      return res.status(401).json({ message: 'Credenciales inválidas' });
    }

    // Login exitoso - enviar datos del usuario
    res.status(200).json({
      message: 'Inicio de sesión exitoso',
      user: {
        id: user.id,
        firstName: user.first_name,
        lastName: user.last_name,
        email: user.email,
        isSuperuser: user.is_superuser === 1,
        agencia: user.Agencia
      }
    });
  } catch (error) {
    console.error('Error en el proceso de login:', error);
    res.status(500).json({ message: 'Error en el servidor' });
  } finally {
    if (connection) connection.release();
  }
});

// Agregar un endpoint para ver estadísticas de caché
app.get('/api/cache/stats', async (req, res) => {
  try {
    const connection = await mysql.createConnection({
      host: process.env.MYSQLHOST || process.env.HOST_DB || 'localhost',
      port: process.env.MYSQLPORT || process.env.PORT_DB || 3306,
      user: process.env.MYSQLUSER || process.env.USER || 'root',
      password: process.env.MYSQLPASSWORD || process.env.PASSWORD || 'root',
      database: process.env.MYSQLDATABASE || process.env.DATABASE || 'railway'
    });

    const [metadata] = await connection.execute('SELECT * FROM cache_metadata');
    const [cacheInfo] = await connection.execute('SELECT COUNT(*) as total, MAX(timestamp) as last_update FROM query_cache');

    await connection.end();

    res.json({
      metadata,
      cacheInfo: cacheInfo[0]
    });
  } catch (error) {
    console.error('Error al obtener estadísticas de caché:', error);
    res.status(500).json({ error: error.message });
  }
});

// Iniciar el servicio de actualización programada
try {
  console.log("Intentando iniciar servicio de actualización programada...");
  const { initScheduleService } = await import('./service/scheduleDataUpdates.js');

  // Iniciar el servicio (que también hará la precarga inicial)
  await initScheduleService();
  console.log("Servicio de actualización programada iniciado correctamente");
} catch (error) {
  console.error("Error al iniciar servicio de actualización programada:", error);

  // Si falla el programador, al menos intentamos cargar los datos una vez
  try {
    console.log("Realizando precarga de datos fallback...");
    await preloadAgencyData();
    console.log("Precarga de fallback completada");
  } catch (fallbackError) {
    console.error("Error en la precarga de fallback:", fallbackError);
  }
}

// Iniciar el servidor
app.listen(port, "0.0.0.0", () => {
  console.log(`Servidor backend Loker ejecutándose en puerto ${port}`);
  console.log(`API disponible en http://localhost:${port}/api`);
});