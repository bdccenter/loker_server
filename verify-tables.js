// verify-tables.js
import mysql from 'mysql2/promise';
import { createConnectionWithRetry } from './server/service/dbConnection.js';

async function verifyAndFixTables() {
  console.log('Verificando estructura de tablas...');
  let connection;
  
  try {
    connection = await createConnectionWithRetry({}, 3, 2000);
    
    // Verificar la estructura actual de la tabla cache_metadata
    const [columns] = await connection.execute(`
      SHOW COLUMNS FROM cache_metadata LIKE 'status'
    `);
    
    if (columns.length > 0) {
      console.log('Estructura actual de la columna status:');
      console.log(columns[0]);
      
      // Modificar la tabla si es necesario para asegurar que acepta todos los valores necesarios
      await connection.execute(`
        ALTER TABLE cache_metadata 
        MODIFY COLUMN status ENUM('success', 'error', 'invalidated', 'updating', 'pending') NOT NULL
      `);
      
      console.log('Tabla cache_metadata actualizada correctamente');
    } else {
      console.log('No se encontró la columna status en la tabla cache_metadata');
    }
    
    console.log('Verificación completada');
  } catch (error) {
    console.error('Error durante la verificación:', error);
  } finally {
    if (connection) await connection.end();
  }
}

verifyAndFixTables();