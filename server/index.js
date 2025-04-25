// src/server/server.js
const  express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const mysql = require('mysql2/promise');

// Crear la aplicación Express
const app = express();
const port = 3001;

// Configuración CORS básica
app.use(cors());

// Middleware para parsear JSON
app.use(express.json());

// Configuración de la conexión a la base de datos MySQL
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'autoinsights',
  connectionLimit: 10
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
    
    // Insertar nuevo usuario
    const [result] = await connection.execute(
      'INSERT INTO users (password, is_superuser, first_name, last_name, email, Agencia) VALUES (?, ?, ?, ?, ?, ?)',
      [hashedPassword, is_superuser ? 1 : 0, first_name, last_name, email, Agencia || null]
    );
    
    // Obtener el usuario recién creado
    const [newUser] = await connection.execute(
      'SELECT id, is_superuser, first_name, last_name, email, Agencia FROM users WHERE id = ?',
      [result.insertId]
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
    connection = await pool.getConnection();
    
    // Verificar si el usuario existe
    const [existingUser] = await connection.execute(
      'SELECT id FROM users WHERE id = ?',
      [userId]
    );
    
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
    
    // Verificar la contraseña (usando bcrypt o el método que uses)
    const hash = crypto.createHash('sha256').update(password).digest('hex');
    const passwordValid = hash === user.password || password === user.password;
    
    if (!passwordValid) {
      return res.status(401).json({ message: 'Credenciales inválidas' });
    }
    
    // Login exitoso - enviar datos del usuario (sin la contraseña)
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
    // Siempre liberar la conexión
    if (connection) connection.release();
  }
});

// Iniciar el servidor
app.listen(port, () => {
  console.log(`Servidor ejecutándose en http://localhost:${port}`);
  console.log(`Conectando a la base de datos en ${dbConfig.host}/${dbConfig.database}`);
});