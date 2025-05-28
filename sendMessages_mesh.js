const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();

// PARAMETROS

// Nome do arquivo CSV contendo os leads que serão lidos, o arquivo deve estar na pasta csv
const CSV_FILE = 'teste.csv';

// Nome do arquivo do banco de dados SQLite que será gerado automaticamente com os dados de envio
const DATABASE_FILE = 'database.db';

// Configuração do horário de funcionamento (segunda a sexta, das 8h às 17h)
// Formato cron: minuto hora dia-do-mês mês dia-da-semana
// 0 8-17 * * 1-5 = das 8h às 17h, segunda a sexta
const CRON_SETTINGS = '0 8-17 * * 1-5';

// Incluir as URLs das APIs Whatsapp aqui
const API_ENDPOINTS = [
    'http://localhost:3001',
    'http://localhost:3002',
    'http://localhost:3003',
    'http://localhost:3004'
];

// INICIO DO SCRIPT

// Caminho do arquivo CSV
const CSV_FILE_PATH = path.join(__dirname, 'csv', CSV_FILE);

// Caminho do arquivo do banco de dados SQLite
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

// Variável global para controlar o índice da API atual
let currentApiIndex = 0;

// Função para verificar se estamos no horário comercial
function isBusinessHours() {
    const now = new Date();
    const dayOfWeek = now.getDay(); // 0 = domingo, 1 = segunda, ..., 6 = sábado
    const hour = now.getHours();
    
    // Segunda a sexta (1-5) das 8h às 17h
    const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
    const isBusinessHour = hour >= 8 && hour <= 17;
    
    return isWeekday && isBusinessHour;
}

// Função para calcular próximo horário comercial
function getNextBusinessTime() {
    const now = new Date();
    const dayOfWeek = now.getDay();
    const hour = now.getHours();
    
    // Se é fim de semana (sábado ou domingo)
    if (dayOfWeek === 0 || dayOfWeek === 6) {
        const nextMonday = new Date(now);
        const daysUntilMonday = dayOfWeek === 0 ? 1 : 2; // Se domingo = 1 dia, se sábado = 2 dias
        nextMonday.setDate(now.getDate() + daysUntilMonday);
        nextMonday.setHours(8, 0, 0, 0);
        return nextMonday;
    }
    
    // Se é dia útil mas fora do horário
    if (hour < 8) {
        // Antes das 8h - aguardar até 8h do mesmo dia
        const nextTime = new Date(now);
        nextTime.setHours(8, 0, 0, 0);
        return nextTime;
    } else if (hour > 17) {
        // Depois das 17h - aguardar até 8h do próximo dia útil
        const nextTime = new Date(now);
        if (dayOfWeek === 5) { // Se é sexta, próximo dia útil é segunda
            nextTime.setDate(now.getDate() + 3);
        } else {
            nextTime.setDate(now.getDate() + 1);
        }
        nextTime.setHours(8, 0, 0, 0);
        return nextTime;
    }
    
    return now; // Já estamos no horário comercial
}

// Variável global para controlar o estado do processamento
let isProcessing = false;
let pendingRows = [];

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
    // Se não estamos mais processando, parar
    if (!isProcessing) {
        console.log('Processamento pausado devido ao horário não comercial.');
        return;
    }

    if (rows.length === 0) {
        console.log('Todas as mensagens foram enviadas.');
        isProcessing = false;
        pendingRows = [];
        return;
    }

    // Verificar se estamos no horário comercial
    if (!isBusinessHours()) {
        const nextBusinessTime = getNextBusinessTime();
        const now = new Date();
        const waitTime = nextBusinessTime.getTime() - now.getTime();
        
        console.log(`Fora do horário comercial. Próximo envio será em: ${nextBusinessTime.toLocaleString()}`);
        console.log(`Aguardando ${Math.round(waitTime / 1000 / 60)} minutos até o próximo horário comercial...`);
        
        isProcessing = false;
        pendingRows = rows; // Salvar as linhas pendentes
        
        // Aguardar até o próximo horário comercial e tentar novamente
        setTimeout(() => {
            if (pendingRows.length > 0) {
                isProcessing = true;
                processRows(pendingRows);
            }
        }, waitTime);
        return;
    }

    const { nome, whatsapp } = rows.shift(); // Remove e obtém o primeiro elemento do array
    pendingRows = rows; // Atualizar as linhas pendentes

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
    console.log(`Restam ${rows.length} mensagens para enviar.`);
    
    setTimeout(() => {
        if (isProcessing) {
            processRows(rows);
        }
    }, delay);
}

// Lendo o arquivo CSV
console.log('Iniciando a leitura do arquivo CSV...');
console.log(`Configuração de horário: ${CRON_SETTINGS} (Segunda a Sexta, 8h às 17h)`);

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
        console.log('Processamento do CSV concluído.');
        console.log(`Total de ${rows.length} registros carregados.`);
        
        // Iniciar o monitor de horário comercial
        startBusinessHoursMonitor();
        
        // Iniciar o processamento com controle de horário
        startProcessing(rows);
    });

// Função para monitorar horário comercial a cada 5 minutos
function startBusinessHoursMonitor() {
    const checkInterval = 5 * 60 * 1000; // 5 minutos em milissegundos
    
    setInterval(() => {
        const now = new Date();
        const inBusinessHours = isBusinessHours();
        
        console.log(`[${now.toLocaleString()}] Verificação de horário comercial: ${inBusinessHours ? 'ATIVO' : 'INATIVO'}`);
        
        if (!inBusinessHours && isProcessing) {
            console.log('Saindo do horário comercial. Pausando envio de mensagens...');
            isProcessing = false;
        } else if (inBusinessHours && !isProcessing && pendingRows.length > 0) {
            console.log('Entrando no horário comercial. Retomando envio de mensagens...');
            isProcessing = true;
            processRows(pendingRows);
        }
    }, checkInterval);
    
    console.log('Monitor de horário comercial iniciado. Verificação a cada 5 minutos.');
}

// Função para iniciar o processamento com controle de horário
function startProcessing(rows) {
    pendingRows = [...rows]; // Copia o array de linhas
    
    if (isBusinessHours()) {
        console.log('Iniciando processamento no horário comercial...');
        isProcessing = true;
        processRows(pendingRows);
    } else {
        const nextBusinessTime = getNextBusinessTime();
        console.log(`Fora do horário comercial. Processamento iniciará em: ${nextBusinessTime.toLocaleString()}`);
        
        // Aguardar até o próximo horário comercial
        const waitTime = nextBusinessTime.getTime() - new Date().getTime();
        setTimeout(() => {
            if (pendingRows.length > 0) {
                console.log('Iniciando processamento no horário comercial...');
                isProcessing = true;
                processRows(pendingRows);
            }
        }, waitTime);
    }
}
