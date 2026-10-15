package com.distribuidos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import smile.classification.KNN;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Consumidor 1 – Análise de Sentimento de avaliações de produtos.
 *
 * Utiliza um classificador KNN (k-Nearest Neighbors) da biblioteca Smile
 * (Statistical Machine Intelligence and Learning Engine), treinado no
 * startup do consumidor com uma base rotulada de avaliações em português.
 */
public class ReviewConsumerApp {
    private static final String EXCHANGE_NAME = "app.topic.exchange";
    private static final String QUEUE_NAME = "reviews.queue";
    private static final String DLQ_EXCHANGE = "dlq.exchange";
    private static final String DLQ_QUEUE = "dlq.queue";
    private static final String HOST = System.getenv().getOrDefault("RABBITMQ_HOST", "rabbitmq");

    /** Rótulos possíveis (índices usados no treinamento do modelo). */
    private static final String[] SENTIMENTOS = { "POSITIVO", "NEGATIVO", "NEUTRO" };

    /** Base de treinamento rotulada: {texto da avaliação, sentimento}. */
    private static final String[][] BASE_TREINO = {
            // ---- POSITIVO ----
            { "excelente produto, superou minhas expectativas e recomendo a todos", "POSITIVO" },
            { "amei a compra, produto maravilhoso e chegou rápido", "POSITIVO" },
            { "ótima qualidade, funciona perfeitamente desde o primeiro dia", "POSITIVO" },
            { "muito bom, recomendo sem hesitar, chegou antes do prazo", "POSITIVO" },
            { "produto excelente, atendeu todas as necessidades da minha casa", "POSITIVO" },
            { "adorei o produto, custo benefício incrível", "POSITIVO" },
            { "ótimo produto e ótimo acabamento, estou muito satisfeito", "POSITIVO" },
            { "funciona muito bem e a entrega foi rápida, feliz com a compra", "POSITIVO" },
            { "melhor compra do ano, recomendo para todos os meus amigos", "POSITIVO" },
            { "produto bom de verdade, material resistente e acabamento caprichado", "POSITIVO" },
            // ---- NEGATIVO ----
            { "péssima qualidade, o produto quebrou no primeiro dia", "NEGATIVO" },
            { "horrível, não comprem esse produto, quebrou rápido", "NEGATIVO" },
            { "produto ruim, veio defeituoso e o suporte não resolve nada", "NEGATIVO" },
            { "decepcionado com a compra, qualidade muito inferior ao anunciado", "NEGATIVO" },
            { "não recomendo, perdi dinheiro com esse produto péssimo", "NEGATIVO" },
            { "péssimo, parou de funcionar depois de uma semana", "NEGATIVO" },
            { "produto com defeito, não liga e a garantia não foi respeitada", "NEGATIVO" },
            { "atendimento horroroso e produto mal acabado, arrependido da compra", "NEGATIVO" },
            { "lixo de produto, nunca mais compro essa marca", "NEGATIVO" },
            { "comprei e me arrependi, produto quebrou rápido e ninguém resolve", "NEGATIVO" },
            // ---- NEUTRO ----
            { "atende as expectativas, nada de mais nem de menos", "NEUTRO" },
            { "produto normal, cumpre o básico, sem grandes diferenças", "NEUTRO" },
            { "mediano, nem bom nem ruim, apenas aceitável pelo preço", "NEUTRO" },
            { "chegou dentro do prazo, funciona como descrito na embalagem", "NEUTRO" },
            { "produto comum, comparável a outros do mesmo segmento", "NEUTRO" },
            { "nem satisfeito nem insatisfeito, produto regular", "NEUTRO" },
            { "instalação simples, produto ok, mas nada excepcional", "NEUTRO" },
            { "esperava mais, mas também esperava menos, avaliação neutra", "NEUTRO" },
            { "produto intermediário, cumpre o prometido sem destacar", "NEUTRO" },
            { "uso razoável, qualidade standard, sem elogios nem reclamações", "NEUTRO" }
    };

    /** Palavras-function (português) ignoradas na vetorização. */
    private static final Set<String> STOPWORDS = Set.of(
            "o", "a", "os", "as", "um", "uma", "uns", "umas", "de", "do", "da", "dos", "das",
            "em", "no", "na", "nos", "nas", "que", "e", "ou", "com", "para", "por", "se", "ao",
            "à", "às", "meu", "minha", "meus", "minhas", "esse", "essa", "este", "esta",
            "the", "and", "of", "to", "is");

    private static KNN<double[]> model;
    private static List<String> vocabulario;
    private static Map<String, Integer> indiceTermo;

    public static void main(String[] args) throws Exception {
        // ---- Treinamento da IA embarcada (ocorre uma vez, no startup) ----
        long t0 = System.currentTimeMillis();
        treinarModelo();
        System.out.printf(" [*] IA (KNN - Smile) treinada com %d exemplos e %d termos em %d ms%n",
                BASE_TREINO.length, vocabulario.size(), System.currentTimeMillis() - t0);

        // ---- Conexão com o RabbitMQ (com retry) ----
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection connection = conectarComRetry(factory);
        Channel channel = connection.createChannel();

        // ---- Declarações: DLQ + fila de avaliações com dead-letter ----
        channel.exchangeDeclare(DLQ_EXCHANGE, "direct", true);
        channel.queueDeclare(DLQ_QUEUE, true, false, false, null);
        channel.queueBind(DLQ_QUEUE, DLQ_EXCHANGE, "dead-letter");

        Map<String, Object> argsQueue = new HashMap<>();
        argsQueue.put("x-dead-letter-exchange", DLQ_EXCHANGE);
        argsQueue.put("x-dead-letter-routing-key", "dead-letter");

        channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
        channel.queueDeclare(QUEUE_NAME, true, false, false, argsQueue);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "review.product");

        channel.basicQos(1);
        System.out.println(" [*] Consumidor 1 (Avaliações) aguardando mensagens na fila '" + QUEUE_NAME + "'...");

        ObjectMapper mapper = new ObjectMapper();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageStr = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                Message msg = mapper.readValue(messageStr, Message.class);

                // ---- Classificação com a IA embarcada ----
                double[] posteriori = new double[SENTIMENTOS.length];
                int melhor = model.predict(vetorizar(msg.getContent()), posteriori);
                String sentimento = SENTIMENTOS[melhor];
                double confianca = posteriori[melhor] * 100;

                System.out.printf(" [Avaliação %s] ID: %s | Sentimento: %s (%.1f%% de confiança)%n     Conteúdo: %s%n",
                        msg.getTimestamp(), msg.getId(), sentimento, confianca, msg.getContent());

                // Processamento deliberadamente lento (350 ms) para visualizar o crescimento da fila
                Thread.sleep(350);

                // ---- Confirmação manual (ACK) somente após o processamento ----
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (Exception e) {
                // ---- Erro de processamento -> NACK -> fila morta (DLQ) ----
                System.err.println(" [!] Erro ao processar mensagem (" + e.getMessage() + "). Enviando para a DLQ: " + DLQ_QUEUE);
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }

    // ------------------------------------------------------------------
    // IA embarcada (Smile) – KNN (k-Nearest Neighbors) sobre Bolsa-de-Palavras
    // ------------------------------------------------------------------

    /** Treina o classificador KNN (k = 1) com a base rotulada. */
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
            y[i] = indiceDe(SENTIMENTOS, BASE_TREINO[i][1]);
        }

        // 3) Aprende (treina) o modelo KNN do Smile
        model = KNN.fit(x, y, 1);
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
