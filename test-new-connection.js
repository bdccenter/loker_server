// test-new-connection.js
import mysql from 'mysql2/promise';

async function testNewConnection() {
  console.log('Probando conexión a la base de datos con nuevas credenciales...');
  
  const connectionConfig = {
    host: 'ballast.proxy.rlwy.net',
    port: 13555,
    user: 'root',
    password: 'mhqtkrjWmDlCpywNtJhGUxsNHimRqwVr',
    database: 'railway',
    connectTimeout: 15000 // 15 segundos de timeout
  };
  
  console.log('Usando credenciales:');
  console.log(`Host: ${connectionConfig.host}`);
  console.log(`Puerto: ${connectionConfig.port}`);
  console.log(`Usuario: ${connectionConfig.user}`);
  console.log(`Base de datos: ${connectionConfig.database}`);
  
  let connection;
  
  try {
    connection = await mysql.createConnection(connectionConfig);
    
    console.log('✅ Conexión exitosa a la base de datos');
    
    // Verificar tablas
    const [tables] = await connection.execute('SHOW TABLES');
    console.log('Tablas en la base de datos:');
    console.table(tables);
    
    await connection.end();
    return true;
  } catch (error) {
    console.error('❌ Error de conexión:', error.message);
    return false;
  }
}

testNewConnection().catch(error => {
  console.error('Error general en la prueba de conexión:', error);
});