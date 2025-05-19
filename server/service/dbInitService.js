// server/service/dbInitService.js (versión mejorada)
import { createConnectionWithRetry } from './dbConnection.js';

export async function initializeDatabase() {
    let connection;

    try {
        console.log('Intentando conectar a la base de datos para inicializar tablas...');
        
        // Usar nuestra nueva función con reintentos (3 intentos, 2 segundos entre intentos)
        connection = await createConnectionWithRetry({
            connectTimeout: 30000 // 30 segundos de timeout
        }, 3, 2000);

        console.log('Conectado a la base de datos, inicializando tablas...');

        // Crear tabla query_cache
        await connection.execute(`
      CREATE TABLE IF NOT EXISTS query_cache (
        cache_key VARCHAR(255) NOT NULL PRIMARY KEY,
        data LONGTEXT NOT NULL,
        timestamp DATETIME NOT NULL
      )
    `);

        // Crear tabla cache_metadata
        await connection.execute(`
      CREATE TABLE IF NOT EXISTS cache_metadata (
        agency VARCHAR(50) NOT NULL PRIMARY KEY,
        last_updated DATETIME NOT NULL,
        record_count INT NULL,
        status ENUM('success', 'error', 'invalidated', 'updating') NOT NULL,
        error_message TEXT NULL
      )
    `);

        // Crear tabla users si no existe
        await connection.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        password VARCHAR(255) NOT NULL,
        is_superuser TINYINT(1) NOT NULL DEFAULT 0,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        Agencia VARCHAR(100) NULL
      )
    `);

        console.log('Tablas inicializadas correctamente');
        return true;
    } catch (error) {
        console.error('Error al inicializar base de datos:', error);
        return false;
    } finally {
        if (connection) {
            try {
                await connection.end();
                console.log('Conexión cerrada correctamente');
            } catch (closeError) {
                console.error('Error al cerrar la conexión:', closeError);
            }
        }
    }
}