package da

import (
    "context"
    "database/sql"
    "encoding/hex"
    "fmt"
    "log"
    "os"
    "os/exec"
    "strings"

    "github.com/ethereum/go-ethereum/ethclient"
    "github.com/Layer-Edge/bitcoin-da/config"
    _ "github.com/mattn/go-sqlite3"
)

var (
    db            *sql.DB
    BtcCliPath    string
    BashScriptPath string
)

func initDB() error {
    var err error
    db, err = sql.Open("sqlite3", "da.db")
    if err != nil {
        return fmt.Errorf("failed to open database: %v", err)
    }

    statement := `CREATE TABLE IF NOT EXISTS writer (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        txnHash TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
    );`
    
    if _, err := db.Exec(statement); err != nil {
        return fmt.Errorf("failed to create table: %v", err)
    }
    
    return nil
}

func CallScriptWithData(data string) ([]byte, error) {
    cmd := exec.Command(BashScriptPath+"/op_return_transaction.sh", data)
    cmd.Env = os.Environ()
    cmd.Env = append(cmd.Env, "BTC_CLI_PATH="+BtcCliPath)
    return cmd.Output()
}

func processHashBlock(msg [][]byte, protocolId string, layerEdgeClient *ethclient.Client) error {
    layerEdgeHeader, err := layerEdgeClient.HeaderByNumber(context.Background(), nil)
    if err != nil {
        return fmt.Errorf("error getting layerEdgeHeader: %v", err)
    }

    dhash := layerEdgeHeader.Hash()
    log.Println("Latest LayerEdge Block Hash:", dhash.Hex())
    
    data := append([]byte(protocolId), dhash.Bytes()...)
    hash, err := CallScriptWithData(hex.EncodeToString(data))
    if err != nil {
        return err
    }

    txHash := strings.TrimSpace(string(hash))
    log.Println("Relayer Write Done ->", txHash)

    statement := `insert into writer(txnHash) values($1);`
    _, err = db.Exec(statement, txHash)
    return err
}

func HashBlockSubscriber(cfg *config.Config) {
    if err := initDB(); err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    BashScriptPath = cfg.BashScriptPath
    BtcCliPath = cfg.BtcCliPath

    subscriber, err := NewZmqSubscriber(cfg.ZmqEndpointHashBlock, "hashblock")
    if err != nil {
        log.Fatal("Failed to create subscriber:", err)
    }
    defer subscriber.Close()

    layerEdgeClient, err := ethclient.Dial(cfg.LayerEdgeRPC.HTTP)
    if err != nil {
        log.Fatal("Error creating layerEdgeClient:", err)
    }

    counter := 0
    handler := func(msg [][]byte) error {
        if (counter % cfg.WriteIntervalBlock) != 0 {
            return nil
        }
        counter++
        return processHashBlock(msg, cfg.ProtocolId, layerEdgeClient)
    }

    subscriber.Listen(handler)
}
