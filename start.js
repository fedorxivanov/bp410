const sqlConnectionPool = require('tedious-connection-pool')
const sqlRequest = require('tedious').Request
const events = require('events')
class emitter extends events {}
const eventHandler = new emitter()

const sqlPoolConfig = {
    min: 2,
    log: false
}

const sqlConnectionConfig = {
    userName: 'sa',
    password: 'password01',
    server: 'localhost',
    port: 1433
}

const sqlPool = new sqlConnectionPool(sqlPoolConfig, sqlConnectionConfig)


sqlPool.on('error', err => {

    throw err
})

runTransaction('tran001',
"delete from bpcli410 where varcharCol01 like '%1%'",
"insert into bpcli410 values (11111111111, 'tran01', 'tran01', 'tran01')")

eventHandler.on('tran001::deleteCompleted', () => {

    runTransaction('tran002',
    "delete from bpcli410 where varcharCol01 like '%2%'",
    "insert into bpcli410 values (2222222222, 'tran02', 'tran02', 'tran02')",
    exitNode)
})


function runTransaction (transactionName, deleteSqlStatement, insertSqlStatement, callback) {

    sqlPool.acquire((err, acquiredConnection) => {

        if (err) {
    
            throw err
        }
        console.info(`${new Date() .toISOString()} :: ${transactionName} :: connection acquired`)
        let deleteRequest = new sqlRequest(deleteSqlStatement, (err, rowCount) => {
    
            if (err) {
    
                throw err
            }
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[1]}" completed. Rows affected: ` + rowCount)
            eventHandler.emit(`${transactionName}::deleteCompleted`)
            acquiredConnection.execSqlBatch(insertRequest)
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[2]}" started`)
        })
        let insertRequest = new sqlRequest(insertSqlStatement, (err, rowCount) => {
    
            if (err) {
    
                throw err
            }
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[2]}" completed. Rows affected: ` + rowCount)
            acquiredConnection.rollbackTransaction(err => {
    
                if (err) {
    
                    throw (err)
                }
                console.info(`${new Date() .toISOString()} :: ${transactionName} :: rollback`)
                if (callback) {

                    callback()
                }
            }, transactionName)
        })
    
        acquiredConnection.beginTransaction(err => {
    
            if (err) {
    
                throw err
            }
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: begin`)
            acquiredConnection.execSqlBatch(deleteRequest)
            console.info(`${new Date() .toISOString()} :: ${transactionName} :: "${arguments[1]}" started`)
        }, transactionName)
    })
}

function exitNode() {

    console.info(`${new Date() .toISOString()} :: terminating NodeJS process`)
    setImmediate(process.exit())
}