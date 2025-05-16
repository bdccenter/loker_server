// server/index.js - Versión ES modules
import express from 'express';
import cors from 'cors';
import crypto from 'crypto';
import mysql from 'mysql2/promise';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';

// Cargar variables de entorno
dotenv.config();

// Importar servicios BigQuery
import { getAgencyData, invalidateCache, preloadAgencyData, agencyConfig, queryCache } from './service/bigQueryDirectService.js';


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

// Configuración CORS básica
app.use(cors());

// Middleware para parsear JSON
app.use(express.json());

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

// Endpoint para obtener datos de una agencia específica
app.get('/api/data/:agencyName', async (req, res) => {
  try {
    const { agencyName } = req.params;
    // Convertir query params a filtros
    const filters = req.query;
    console.log(`Solicitud de datos para agencia: ${agencyName}, filtros:`, filters);

    // Forzar el uso de caché siempre que sea posible
    const useCache = true;

    const data = await getAgencyData(agencyName, filters, useCache);
    console.log(`Datos obtenidos para ${agencyName}: ${data.length} registros`);

    res.json(data);
  } catch (error) {
    console.error('Error en endpoint /api/data:', error);
    res.status(500).json({ error: error.message });
  }
});

// Añadir el resto de endpoints de BigQuery
app.post('/api/cache/invalidate/:agencyName', (req, res) => {
  try {
    const { agencyName } = req.params;
    console.log(`Invalidando caché para agencia: ${agencyName}`);

    invalidateCache(agencyName);

    res.json({ success: true, message: `Caché invalidada para ${agencyName}` });
  } catch (error) {
    console.error('Error en endpoint /api/cache/invalidate:', error);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint para actualizar manualmente los datos
app.post('/api/update', async (req, res) => {
  try {
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
  try {
    // Obtener una conexión del pool
    connection = await pool.getConnection();

    // Consultar los usuarios
    const [rows] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users'
    );

    res.json({ users: rows });
  } catch (error) {
    console.error('Error al obtener usuarios:', error);
    res.status(500).json({ message: 'Error al obtener usuarios' });
  } finally {
    if (connection) connection.release();
  }
});

// GET - Obtener un usuario específico
app.get('/users/:id', async (req, res) => {
  const userId = parseInt(req.params.id);
  let connection;

  try {
    connection = await pool.getConnection();

    const [rows] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users WHERE id = ?',
      [userId]
    );

    if (rows.length === 0) {
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