package com.distribuidos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import smile.classification.Classifier;
import smile.classification.LogisticRegression;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Consumidor 2 – Classificação de solicitações de suporte.
 *
 * Utiliza um classificador de Regressão Logística Multinomial da biblioteca
 * Smile, treinado no startup do consumidor com uma base rotulada de
 * solicitações de suporte em português, separando-as em três categorias:
 * problema de acesso, problema de desempenho e problema de conexão.
 */
public class SupportConsumerApp {
    private static final String EXCHANGE_NAME = "app.topic.exchange";
    private static final String QUEUE_NAME = "support.queue";
    private static final String DLQ_EXCHANGE = "dlq.exchange";
    private static final String DLQ_QUEUE = "dlq.queue";
    private static final String HOST = System.getenv().getOrDefault("RABBITMQ_HOST", "rabbitmq");

    /** Rótulos possíveis (índices usados no treinamento do modelo). */
    private static final String[] CATEGORIAS = { "Problema de acesso", "Problema de desempenho", "Problema de conexão" };

    /** Base de treinamento rotulada: {texto da solicitação, categoria}. */
    private static final String[][] BASE_TREINO = {
            // ---- PROBLEMA DE ACESSO ----
            { "não consigo fazer login na minha conta", "Problema de acesso" },
            { "esqueci minha senha e o e-mail de recuperação não chega", "Problema de acesso" },
            { "meu usuário está bloqueado e não consigo acessar o sistema", "Problema de acesso" },
            { "a tela de login mostra erro de credenciais mesmo com a senha correta", "Problema de acesso" },
            { "não consigo criar minha conta, o cadastro falha no meio", "Problema de acesso" },
            { "token de autenticação expirou e não consigo entrar novamente", "Problema de acesso" },
            { "permissão negada ao tentar acessar o painel administrativo", "Problema de acesso" },
            { "o sistema não reconhece meu login e senha depois da atualização", "Problema de acesso" },
            { "problema para recuperar o acesso da conta da minha empresa", "Problema de acesso" },
            { "duplo fator de autenticação não está sendo enviado para o celular", "Problema de acesso" },
            // ---- PROBLEMA DE DESEMPENHO ----
            { "o sistema está muito lento e demora para carregar as páginas", "Problema de desempenho" },
            { "as páginas demoram eternas para abrir durante o horário comercial", "Problema de desempenho" },
            { "relatórios demoram minutos para serem gerados, lentidão extrema", "Problema de desempenho" },
            { "a aplicação trava e fica com lentidão ao abrir vários módulos", "Problema de desempenho" },
            { "tempo de resposta altíssimo para cada clique no sistema", "Problema de desempenho" },
            { "o dashboard carrega devagar e às vezes congela a tela", "Problema de desempenho" },
            { "performance péssima depois da última atualização do sistema", "Problema de desempenho" },
            { "processamento de dados está lento e estoura o tempo limite", "Problema de desempenho" },
            { "sistema engasgando ao salvar planilhas grandes", "Problema de desempenho" },
            { "demora absurda para exportar arquivos em PDF", "Problema de desempenho" },
            // ---- PROBLEMA DE CONEXÃO ----
            { "minha conexão cai toda vez que tento salvar o formulário", "Problema de conexão" },
            { "erro de timeout de conexão ao conectar com o banco de dados", "Problema de conexão" },
            { "a internet do sistema cai intermitentemente durante o uso", "Problema de conexão" },
            { "não consigo conectar ao servidor, a conexão é recusada", "Problema de conexão" },
            { "erro 502 de gateway, conexão perdida com o servidor remoto", "Problema de conexão" },
            { "perda de conexão com a API externa a cada poucos minutos", "Problema de conexão" },
            { "falha de rede ao sincronizar dados com o servidor", "Problema de conexão" },
            { "conexão com o banco fica instável e derruba a aplicação", "Problema de conexão" },
            { "ssl handshake falha ao conectar com o site", "Problema de conexão" },
            { "conexao websocket desconecta sozinha sempre", "Problema de conexão" }
    };

    /** Palavras-function (português) ignoradas na vetorização. */
    private static final Set<String> STOPWORDS = Set.of(
            "o", "a", "os", "as", "um", "uma", "uns", "umas", "de", "do", "da", "dos", "das",
            "em", "no", "na", "nos", "nas", "que", "e", "ou", "com", "para", "por", "se", "ao",
            "à", "às", "meu", "minha", "meus", "minhas", "esse", "essa", "este", "esta",
            "the", "and", "of", "to", "is");

    private static Classifier<double[]> model;
    private static List<String> vocabulario;
    private static Map<String, Integer> indiceTermo;

    public static void main(String[] args) throws Exception {
        // ---- Treinamento da IA embarcada (ocorre uma vez, no startup) ----
        long t0 = System.currentTimeMillis();
        treinarModelo();
        System.out.printf(" [*] IA (Regressão Logística - Smile) treinada com %d exemplos e %d termos em %d ms%n",
                BASE_TREINO.length, vocabulario.size(), System.currentTimeMillis() - t0);

        // ---- Conexão com o RabbitMQ (com retry) ----
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection connection = conectarComRetry(factory);
        Channel channel = connection.createChannel();

        // ---- Declarações: DLQ + fila de suporte com dead-letter ----
        channel.exchangeDeclare(DLQ_EXCHANGE, "direct", true);
        channel.queueDeclare(DLQ_QUEUE, true, false, false, null);
        channel.queueBind(DLQ_QUEUE, DLQ_EXCHANGE, "dead-letter");

        Map<String, Object> argsQueue = new HashMap<>();
        argsQueue.put("x-dead-letter-exchange", DLQ_EXCHANGE);
        argsQueue.put("x-dead-letter-routing-key", "dead-letter");

        channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
        channel.queueDeclare(QUEUE_NAME, true, false, false, argsQueue);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "support.request");

        channel.basicQos(1);
        System.out.println(" [*] Consumidor 2 (Suporte) aguardando mensagens na fila '" + QUEUE_NAME + "'...");

        ObjectMapper mapper = new ObjectMapper();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageStr = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                Message msg = mapper.readValue(messageStr, Message.class);

                // ---- Classificação com a IA embarcada ----
                double[] posteriori = new double[CATEGORIAS.length];
                int melhor = model.predict(vetorizar(msg.getContent()), posteriori);
                String categoria = CATEGORIAS[melhor];
                double confianca = posteriori[melhor] * 100;

                System.out.printf(" [Suporte %s] ID: %s | Categoria: %s (%.1f%% de confiança)%n     Conteúdo: %s%n",
                        msg.getTimestamp(), msg.getId(), categoria, confianca, msg.getContent());

                // Processamento deliberadamente lento (450 ms) para visualizar o crescimento da fila
                Thread.sleep(450);

                // ---- Confirmação manual (ACK) somente após o processamento ----
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (Exception e) {
                // ---- Erro de processamento -> NACK -> fila morta (DLQ) ----
                System.err.println(" [!] Erro ao processar mensagem de suporte (" + e.getMessage() + "). Enviando para a DLQ: " + DLQ_QUEUE);
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }

    // ------------------------------------------------------------------
    // IA embarcada (Smile) – Regressão Logística sobre Bolsa-de-Palavras
    // ------------------------------------------------------------------

    /** Treina o classificador de Regressão Logística com a base rotulada. */
    private static void treinarModelo() {
        // 1) Constrói o vocabulário a partir da base de treinamento
        vocabulario = new ArrayList<>();
        indiceTermo = new HashMap<>();
        for (String[] exemplo : BASE_TREINO) {
            for (String token : tokenizar(exemplo[0])) {
                if (!indiceTermo.containsKey(token)) {
                    indiceTermo.put(token, vocabulario.size());
                    vocabulario.add(token);
                }
            }
        }

        // 2) Vetoriza a base (Bolsa-de-Palavras com contagem de termos)
        double[][] x = new double[BASE_TREINO.length][];
        int[] y = new int[BASE_TREINO.length];
        for (int i = 0; i < BASE_TREINO.length; i++) {
            x[i] = vetorizar(BASE_TREINO[i][0]);
            y[i] = indiceDe(CATEGORIAS, BASE_TREINO[i][1]);
        }

        // 3) Aprende (treina) o modelo de Regressão Logística do Smile
        model = LogisticRegression.fit(x, y);
    }

    /** Converte um texto em vetor Bolsa-de-Palavras sobre o vocabulário de treino. */
    private static double[] vetorizar(String texto) {
        double[] vetor = new double[vocabulario.size()];
        for (String token : tokenizar(texto)) {
            Integer idx = indiceTermo.get(token);
            if (idx != null) {         // palavras fora do vocabulário são ignoradas
                vetor[idx] += 1.0;
            }
        }
        return vetor;
    }

    /** Normaliza (minúsculas, sem acentos) e tokeniza o texto. */
    private static List<String> tokenizar(String texto) {
        String limpo = Normalizer.normalize(texto.toLowerCase(), Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .replaceAll("[^a-z0-9]+", " ")
                .trim();
        List<String> tokens = new ArrayList<>();
        for (String t : limpo.split("\\s+")) {
            if (t.length() >= 3 && !STOPWORDS.contains(t)) {
                tokens.add(t);
            }
        }
        return tokens;
    }

    private static int indiceDe(String[] rótulos, String rotulo) {
        for (int i = 0; i < rótulos.length; i++) {
            if (rótulos[i].equals(rotulo)) return i;
        }
        throw new IllegalArgumentException("Rótulo desconhecido: " + rotulo);
    }

    private static Connection conectarComRetry(ConnectionFactory factory) throws Exception {
        while (true) {
            try {
                return factory.newConnection();
            } catch (Exception e) {
                System.out.println(" [.] Aguardando o RabbitMQ ficar disponível... (" + e.getMessage() + ")");
                Thread.sleep(2000);
            }
        }
    }
}
