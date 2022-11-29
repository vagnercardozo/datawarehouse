const mysql = require("mysql");
const moment = require("moment");
const { duration } = require("moment");

const con = mysql.createConnection({
  host: "relational.fit.cvut.cz",
  user: "guest",
  password: "relational",
  database: "financial",
  port: 3306,
});

const conMy = mysql.createConnection({
  host: "127.0.0.1",
  user: "root",
  password: "",
  database: "datawarehouse",
  port: 3306,
});

con.connect(function (err) {
  if (err) throw err;
  console.log("Connected!");
});

conMy.connect(function (err) {
  if (err) throw err;
  console.log("Connected my!");
});

let tempo_id;
let client_id;
let conta_id;
let status_id;

// fazer insert dos 4 status padrões para usarmos os ids depois, respectivamente “Solicitação Pendente”, “Solicitação Aprovada”, "Empréstimo Realizado”, "Empréstimo Liquidado"

//percorre emprestimos existentes
con.query("select * from loan limit 3", function (err, loans) {
  if (err) throw err;
  for (const loan of loans) {
    //encontra cliente referente
    con.query(
      `select * from client inner join district d on d.district_id = client.district_id where client_id = (select client_id from disp where type = 'owner' and account_id = ${loan.account_id} limit 1) 
    `,
      function (err, client) {
        if (err) throw err;

        client = client[0];
        const dimensionClient = {
          idade: moment().diff(moment(client.birth_date), "years"),
          cidade: client.A2,
          estado: client.A3,
        };
        // insert do cliente
        conMy.query(
          `INSERT INTO cliente (cidade, estado, idade) VALUES ("${dimensionClient.cidade}", "${dimensionClient.estado}", "${dimensionClient.idade}")`,
          function (err, client) {
            if (err) throw err;
            client_id = client.insertId;
          }
        );

        // objeto tratado para insert
        const dimensionTime = {
          dia: moment(loan.date).format("DD"),
          dia_semana: moment(loan.date).day(),
          mes: moment(loan.date).format("MM"),
        };
        //Insert de tempo
        conMy.query(
          `INSERT INTO tempo (dia, dia_semana, mes) VALUES (${dimensionTime.dia}, "${dimensionTime.dia_semana}", ${dimensionTime.mes})`,
          function (err, tempo) {
            if (err) throw err;
            tempo_id = tempo.insertId;

            let status_id;
            if (loan.status === "A") {
              status_id = 1;
            }
            if (loan.status === "B") {
              status_id = 2;
            }
            if (loan.status === "C") {
              status_id = 3;
            }
            if (loan.status === "D") {
              status_id = 4;
            }


          }
        );

        //Select o valor total de transações da conta
        con.query(
          `select sum(amount) from trans where account_id = ${loan.account_id}`,
          function (err, trans) {
            if (err) throw err;
            // objeto tratado para insert
            const dimensionAccount = {
              valor_total_movimentacao: "",
              idade_conta: "",
            };
            dimensionAccount.valor_total_movimentacao =
              trans[0]["sum(amount)"];



          }
        );

        //encontra idade da conta
        con.query(
          `select account.date from account where account_id = ${loan.account_id}`,
          function (err, account) {
            if (err) throw err;
            const data2 = new Date();
            const data1 = account[0].date;
            const idade =
              (data2.getFullYear() - data1.getFullYear()) * 12 +
              (data2.getMonth() - data1.getMonth());
            dimensionAccount.idade_conta = idade;


          }
        );

        // fazer o insert da conta
        conMy.query(
          `INSERT INTO conta (valor_total_movimentacao, idade_conta) VALUES (${dimensionAccount.valor_total_movimentacao}, ${dimensionAccount.idade_conta})`,
          function (err, conta) {
            if (err) throw err;
            conta_id = conta.insertId;
          }
        );

        //faz o insert da tabela de fatos
        conMy.query(`INSERT INTO emprestimos (valor, duracao,tempo_id, cliente_id, conta_id, status_id) VALUES (${loan.amount}, ${loan.duration}, ${tempo_id}, ${client_id}, ${conta_id}, ${status_id})`,
          function (err, emprestimos) {
            if (err) throw err;
            console.log('finalizado')

          }
        );
      }
    );
  }
});
