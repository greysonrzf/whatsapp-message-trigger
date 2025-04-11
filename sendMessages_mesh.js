const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();

const CSV_FILE = 'teste.csv';

const DATABASE_FILE = 'database.db';

const API_ENDPOINTS = [
    'http://localserver.tortoise-sirius.ts.net:3001',
    'http://localserver.tortoise-sirius.ts.net:3002',
    'http://localserver.tortoise-sirius.ts.net:3003',
    'http://localserver.tortoise-sirius.ts.net:3004'
];

// Caminho do arquivo CSV
const CSV_FILE_PATH = path.join(__dirname, 'csv', CSV_FILE);

// Configuração do banco de dados SQLite
const dbPath = path.join(__dirname, 'db', DATABASE_FILE);
const db = new sqlite3.Database(dbPath);

// Criando a tabela caso não exista
db.serialize(() => {
    console.log('Criando/verificando a existência da tabela "leaddata"...');
    db.run(`
        CREATE TABLE IF NOT EXISTS leaddata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            telefone TEXT NOT NULL,
            status TEXT NOT NULL,
            data TEXT NOT NULL
        )
    `, () => {
        console.log('Tabela "leaddata" pronta.');
    });
});


let currentApiIndex = 0;

// Função para verificar autenticação da API
async function checkApiAuth(apiBaseUrl) {
    try {
        const response = await axios.get(`${apiBaseUrl}/check-auth`);
        return response.status === 200 && response.data.authenticated === true;
    } catch (error) {
        console.error(`Erro ao verificar autenticação da API ${apiBaseUrl}:`, error.message);
        return false;
    }
}

// Função para obter próxima API válida
async function getNextValidApi() {
    const initialIndex = currentApiIndex;
    
    do {
        const apiBaseUrl = API_ENDPOINTS[currentApiIndex];
        const isValid = await checkApiAuth(apiBaseUrl);
        
        if (isValid) {
            console.log(`API válida encontrada: ${apiBaseUrl}`);
            return apiBaseUrl;
        }
        
        console.log(`API ${apiBaseUrl} não está autenticada, tentando próxima...`);
        currentApiIndex = (currentApiIndex + 1) % API_ENDPOINTS.length;
        
        // Se já verificamos todas as APIs e voltamos ao início
        if (currentApiIndex === initialIndex) {
            throw new Error('Nenhuma API disponível para envio de mensagens');
        }
    } while (true);
}

// Função para personalizar a mensagem com o nome do estabelecimento
function personalizedMessage(fantasyName){
    const message = `Olá, tudo bem?`

    return message;
}

// Função para formatar o número de telefone
function formatPhoneNumber(phone) {
    if (!phone) {
        throw new Error('Número de telefone inválido.');
    }
    console.log(`Formatando o telefone: ${phone}`);
    return `55${phone.replace(/\D/g, '')}`;
}

// Função para verificar se o número já existe na base de dados
function checkIfPhoneExists(phone) {
    return new Promise((resolve, reject) => {
        db.get('SELECT telefone FROM leaddata WHERE telefone = ?', [phone], (err, row) => {
            if (err) {
                return reject(err);
            }
            resolve(!!row); // Retorna true se o telefone existir, false caso contrário
        });
    });
}

function capitalizeText(text) {
    const lowerWords = ['é', 'a', 'o', 'e', 'de', 'da', 'do', 'dos', 'das', 'em', 'no', 'na', 'nos', 'nas', 'com', 'por', 'para', 'se', 'que'];

    const firstName = text.split(' ')[0].toLowerCase(); // Pega apenas o primeiro nome
    return firstName.charAt(0).toUpperCase() + firstName.slice(1); // Capitaliza o primeiro nome
}

// Função para enviar mensagem via API
async function sendMessage(name, phone, message) {
    const requestBody = {
        phones: [phone],
        message
    };

    console.log(`Enviando mensagem para ${name} (${phone})... Mensagem: "${message}"`);

    try {
        // Obtém uma API válida para envio
        const apiBaseUrl = await getNextValidApi();
        
        // Envia a mensagem usando a API válida
        await axios.post(`${apiBaseUrl}/send-message`, requestBody);
        
        // Atualiza o índice para a próxima API
        currentApiIndex = (currentApiIndex + 1) % API_ENDPOINTS.length;
        
        const now = new Date().toISOString();

        // Grava a mensagem como "enviado" no banco de dados
        db.run('INSERT INTO leaddata (nome, telefone, status, data) VALUES (?, ?, ?, ?)', 
            [name, phone, 'enviado', now], 
            () => {
                console.log(`Mensagem enviada e registrada para ${name} (${phone}) usando API: ${apiBaseUrl}`);
            }
        );
    } catch (error) {
        console.error(`Erro ao enviar mensagem para ${name} (${phone}):`, error.message);
        return;
    }
}

// Função para processar as linhas do CSV em sequência
async function processRows(rows) {
    if (rows.length === 0) {
        console.log('Todas as mensagens foram enviadas.');
        return;
    }

    const { nome, whatsapp } = rows.shift(); // Remove e obtém o primeiro elemento do array

    // Formata o número e verifica se já existe na base de dados
    const formattedPhone = formatPhoneNumber(whatsapp);
    const phoneExists = await checkIfPhoneExists(formattedPhone);
    const fantasyName = capitalizeText(nome);
    const messageToSend = personalizedMessage(fantasyName)

    console.log('Número Formatado: ', formattedPhone, formattedPhone.length)

    if (formattedPhone.length < 13) {
        console.log(`Telefone ${formattedPhone} com menos que 10 digitos. Mensagens não serão enviadas para ${nome}.`);
        return processRows(rows); // Processa a próxima linha sem enviar mensagens
    }

    if (phoneExists) {
        console.log(`Telefone ${formattedPhone} já existe na base de dados. Mensagens não serão enviadas para ${nome}.`);
        return processRows(rows); // Processa a próxima linha sem enviar mensagens
    }
    
    await sendMessage(nome, formattedPhone, messageToSend);
        
    // Aguarda um intervalo aleatório antes de processar a próxima linha
    const delay = Math.floor(Math.random() * 200000); // + 120000;
    const now = new Date();
    const nextExecutionTime = new Date(now.getTime() + delay);
    console.log(`A próxima mensagem será enviada em: ${nextExecutionTime.toLocaleTimeString()}. Intervalo de ${(delay / 1000 / 60).toFixed(2)} minutos.`);
    setTimeout(() => processRows(rows), delay);
}

// Lendo o arquivo CSV
console.log('Iniciando a leitura do arquivo CSV...');

const rows = [];

fs.createReadStream(CSV_FILE_PATH)
    .pipe(csv({ separator: ';', headers: ['nome', 'whatsapp'] }))
    .on('data', (row) => {
        const { nome, whatsapp } = row;
        if (nome && whatsapp) {
            rows.push(row);
            console.log(`Linha carregada: ${nome}, ${whatsapp}`);
        }
    })
    .on('end', () => {
        console.log('Processamento do CSV concluído. Iniciando o envio de mensagens...');
        processRows(rows); // Inicia o processamento das linhas
    });
