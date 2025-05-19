// test-with-retry.js
import { createConnectionWithRetry } from './server/service/dbConnection.js';

async function testConnectionWithRetry() {
  console.log('Probando conexión a la base de datos con reintentos...');
  
  try {
    // Probar con 3 reintentos y 3 segundos entre intentos
    const connection = await createConnectionWithRetry({
      host: 'ballast.proxy.rlwy.net',
      port: 13555,
      user: 'root',
      password: 'mhqtkrjWmDlCpywNtJhGUxsNHimRqwVr',
      database: 'railway'
    }, 3, 3000);
    
    console.log('✅ Conexión exitosa a la base de datos');
    
    // Verificar tablas
    const [tables] = await connection.execute('SHOW TABLES');
    console.log('Tablas en la base de datos:');
    console.table(tables);
    
    // Verificar si hay usuarios
    try {
      const [users] = await connection.execute('SELECT id, first_name, last_name, email FROM users LIMIT 5');
      console.log('Usuarios en la base de datos:');
      console.table(users);
    } catch (error) {
      console.error('Error al consultar usuarios:', error.message);
    }
    
    await connection.end();
    console.log('Conexión cerrada correctamente');
    return true;
  } catch (error) {
    console.error('❌ Error final de conexión:', error.message);
    return false;
  }
}

testConnectionWithRetry().then(success => {
  if (success) {
    console.log('Test de conexión completado con éxito');
  } else {
    console.error('Test de conexión falló');
  }
}).catch(error => {
  console.error('Error en test de conexión:', error);
});