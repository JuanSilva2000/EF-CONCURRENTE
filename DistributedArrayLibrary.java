// ====================== INTERFAZ PRINCIPAL ======================

import java.util.*;
import java.util.concurrent.*;
import java.net.*;
import java.io.*;

interface DArrayOperations<T> {
    void distribute(String[] nodeAddresses, int[] ports);
    void applyOperation(ArrayFunction<T> operation);
    T[] collectResults();
    void setReplicationFactor(int factor);
}

@FunctionalInterface
interface ArrayFunction<T> {
    T apply(T element, int index);
}

class DistributedNode implements Runnable {
    private final String nodeId;
    private final int port;
    private final Map<String, Object[]> segments = new ConcurrentHashMap<>();
    private final Map<String, Boolean> nodeStatus = new ConcurrentHashMap<>();
    private final ExecutorService threadPool;
    private volatile boolean running = true;
    private ServerSocket serverSocket;
    
    public DistributedNode(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
        this.threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        startHeartbeatMonitor();
    }
    
    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Node " + nodeId + " started on port " + port);
            
            while (running) {
                Socket client = serverSocket.accept();
                threadPool.submit(() -> handleClient(client));
            }
        } catch (IOException e) {
            System.err.println("Node " + nodeId + " error: " + e.getMessage());
        }
    }
    
    private void handleClient(Socket client) {
        try (ObjectInputStream in = new ObjectInputStream(client.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream())) {
            
            String command = (String) in.readObject();
            
            switch (command) {
                case "STORE_SEGMENT":
                    String segmentId = (String) in.readObject();
                    Object[] data = (Object[]) in.readObject();
                    segments.put(segmentId, data);
                    out.writeObject("STORED");
                    System.out.println("Node " + nodeId + " stored segment " + segmentId);
                    break;
                    
                case "PROCESS_SEGMENT":
                    processSegment(in, out);
                    break;
                    
                case "HEARTBEAT":
                    out.writeObject("ALIVE");
                    break;
                    
                case "GET_SEGMENT":
                    String reqSegmentId = (String) in.readObject();
                    out.writeObject(segments.get(reqSegmentId));
                    break;
            }
        } catch (Exception e) {
            System.err.println("Node " + nodeId + " client error: " + e.getMessage());
        }
    }
    
    private void processSegment(ObjectInputStream in, ObjectOutputStream out) throws Exception {
        String segmentId = (String) in.readObject();
        String operationType = (String) in.readObject();
        
        Object[] segment = segments.get(segmentId);
        if (segment == null) {
            out.writeObject("SEGMENT_NOT_FOUND");
            return;
        }
        
        // Procesamiento paralelo por núcleo
        int numThreads = Runtime.getRuntime().availableProcessors();
        int chunkSize = segment.length / numThreads;
        CountDownLatch latch = new CountDownLatch(numThreads);
        
        for (int i = 0; i < numThreads; i++) {
            final int start = i * chunkSize;
            final int end = (i == numThreads - 1) ? segment.length : (i + 1) * chunkSize;
            
            threadPool.submit(() -> {
                try {
                    for (int j = start; j < end; j++) {
                        segment[j] = applyOperation(segment[j], j, operationType);
                    }
                } catch (Exception e) {
                    System.err.println("Thread error in node " + nodeId + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        out.writeObject("PROCESSED");
        System.out.println("Node " + nodeId + " processed segment " + segmentId);
    }
    
    private Object applyOperation(Object element, int index, String operationType) {
        switch (operationType) {
            case "MATH_COMPLEX":
                if (element instanceof Double) {
                    double x = (Double) element;
                    return Math.pow(Math.sin(x) + Math.cos(x), 2) / (Math.sqrt(Math.abs(x)) + 1);
                }
                break;
            case "CONDITIONAL":
                if (element instanceof Integer) {
                    int x = (Integer) element;
                    if (x % 3 == 0 || (x >= 500 && x <= 1000)) {
                        return (int) ((x * Math.log(x)) % 7);
                    }
                }
                break;
        }
        return element;
    }
    
    private void startHeartbeatMonitor() {
        Thread heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(5000); // Heartbeat cada 5 segundos
                    // Aquí se enviarían heartbeats a otros nodos
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }
    
    public void shutdown() {
        running = false;
        threadPool.shutdown();
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }
    }
}

// ====================== DARRAY DOUBLE ======================
class DArrayDouble implements DArrayOperations<Double> {
    private Double[] data;
    private String[] nodeAddresses;
    private int[] ports;
    private int replicationFactor = 2;
    private Map<String, String[]> replicas = new HashMap<>();
    
    public DArrayDouble(double[] initialData) {
        this.data = new Double[initialData.length];
        for (int i = 0; i < initialData.length; i++) {
            this.data[i] = initialData[i];
        }
    }
    
    @Override
    public void distribute(String[] nodeAddresses, int[] ports) {
        this.nodeAddresses = nodeAddresses;
        this.ports = ports;
        
        int segmentSize = data.length / nodeAddresses.length;
        
        for (int i = 0; i < nodeAddresses.length; i++) {
            int start = i * segmentSize;
            int end = (i == nodeAddresses.length - 1) ? data.length : (i + 1) * segmentSize;
            
            Double[] segment = Arrays.copyOfRange(data, start, end);
            String segmentId = "segment_" + i;
            
            // Enviar segmento al nodo principal
            sendSegmentToNode(nodeAddresses[i], ports[i], segmentId, segment);
            
            // Crear réplicas
            for (int r = 1; r < replicationFactor; r++) {
                int replicaNode = (i + r) % nodeAddresses.length;
                sendSegmentToNode(nodeAddresses[replicaNode], ports[replicaNode], 
                                segmentId + "_replica_" + r, segment);
            }
        }
    }
    
    private void sendSegmentToNode(String address, int port, String segmentId, Double[] segment) {
        try (Socket socket = new Socket(address, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            
            out.writeObject("STORE_SEGMENT");
            out.writeObject(segmentId);
            out.writeObject(segment);
            
            String response = (String) in.readObject();
            System.out.println("Segment " + segmentId + " stored: " + response);
            
        } catch (Exception e) {
            System.err.println("Error sending segment to " + address + ":" + port + " - " + e.getMessage());
        }
    }
    
    @Override
    public void applyOperation(ArrayFunction<Double> operation) {
        // Implementación simplificada usando operaciones predefinidas
        applyMathComplexOperation();
    }
    
    public void applyMathComplexOperation() {
        CountDownLatch latch = new CountDownLatch(nodeAddresses.length);
        List<String> failedNodes = new ArrayList<>();
        
        for (int i = 0; i < nodeAddresses.length; i++) {
            final int nodeIndex = i;
            final String segmentId = "segment_" + i;
            
            Thread worker = new Thread(() -> {
                try {
                    if (!processSegmentOnNode(nodeAddresses[nodeIndex], ports[nodeIndex], segmentId, "MATH_COMPLEX")) {
                        synchronized (failedNodes) {
                            failedNodes.add(nodeAddresses[nodeIndex]);
                        }
                        // Intentar recuperación con réplica
                        recoverFromReplica(segmentId, "MATH_COMPLEX");
                    }
                } catch (Exception e) {
                    System.err.println("Error processing on node " + nodeIndex + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
            worker.start();
        }
        
        try {
            latch.await();
            if (!failedNodes.isEmpty()) {
                System.out.println("Recovered from failed nodes: " + failedNodes);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private boolean processSegmentOnNode(String address, int port, String segmentId, String operationType) {
        try (Socket socket = new Socket(address, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            
            out.writeObject("PROCESS_SEGMENT");
            out.writeObject(segmentId);
            out.writeObject(operationType);
            
            String response = (String) in.readObject();
            return "PROCESSED".equals(response);
            
        } catch (Exception e) {
            System.err.println("Node " + address + ":" + port + " failed: " + e.getMessage());
            return false;
        }
    }
    
    private void recoverFromReplica(String segmentId, String operationType) {
        for (int r = 1; r < replicationFactor; r++) {
            String replicaId = segmentId + "_replica_" + r;
            // Intentar procesar en nodo de réplica
            for (int i = 0; i < nodeAddresses.length; i++) {
                if (processSegmentOnNode(nodeAddresses[i], ports[i], replicaId, operationType)) {
                    System.out.println("Recovered using replica " + replicaId);
                    return;
                }
            }
        }
    }
    
    @Override
    public Double[] collectResults() {
        Double[] results = new Double[data.length];
        int segmentSize = data.length / nodeAddresses.length;
        
        for (int i = 0; i < nodeAddresses.length; i++) {
            String segmentId = "segment_" + i;
            Double[] segment = getSegmentFromNode(nodeAddresses[i], ports[i], segmentId);
            
            if (segment != null) {
                int start = i * segmentSize;
                System.arraycopy(segment, 0, results, start, segment.length);
            }
        }
        
        return results;
    }
    
    private Double[] getSegmentFromNode(String address, int port, String segmentId) {
        try (Socket socket = new Socket(address, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            
            out.writeObject("GET_SEGMENT");
            out.writeObject(segmentId);
            
            return (Double[]) in.readObject();
            
        } catch (Exception e) {
            System.err.println("Error retrieving segment from " + address + ":" + port);
            return null;
        }
    }
    
    @Override
    public void setReplicationFactor(int factor) {
        this.replicationFactor = factor;
    }
}

// ====================== DARRAY INT ======================
class DArrayInt implements DArrayOperations<Integer> {
    private Integer[] data;
    private String[] nodeAddresses;
    private int[] ports;
    private int replicationFactor = 2;
    
    public DArrayInt(int[] initialData) {
        this.data = new Integer[initialData.length];
        for (int i = 0; i < initialData.length; i++) {
            this.data[i] = initialData[i];
        }
    }
    
    @Override
    public void distribute(String[] nodeAddresses, int[] ports) {
        this.nodeAddresses = nodeAddresses;
        this.ports = ports;
        
        int segmentSize = data.length / nodeAddresses.length;
        
        for (int i = 0; i < nodeAddresses.length; i++) {
            int start = i * segmentSize;
            int end = (i == nodeAddresses.length - 1) ? data.length : (i + 1) * segmentSize;
            
            Integer[] segment = Arrays.copyOfRange(data, start, end);
            String segmentId = "segment_" + i;
            
            sendSegmentToNode(nodeAddresses[i], ports[i], segmentId, segment);
        }
    }
    
    private void sendSegmentToNode(String address, int port, String segmentId, Integer[] segment) {
        try (Socket socket = new Socket(address, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            
            out.writeObject("STORE_SEGMENT");
            out.writeObject(segmentId);
            out.writeObject(segment);
            
            String response = (String) in.readObject();
            System.out.println("Segment " + segmentId + " stored: " + response);
            
        } catch (Exception e) {
            System.err.println("Error sending segment: " + e.getMessage());
        }
    }
    
    @Override
    public void applyOperation(ArrayFunction<Integer> operation) {
        applyConditionalOperation();
    }
    
    public void applyConditionalOperation() {
        CountDownLatch latch = new CountDownLatch(nodeAddresses.length);
        
        for (int i = 0; i < nodeAddresses.length; i++) {
            final int nodeIndex = i;
            final String segmentId = "segment_" + i;
            
            Thread worker = new Thread(() -> {
                try {
                    processSegmentOnNode(nodeAddresses[nodeIndex], ports[nodeIndex], segmentId, "CONDITIONAL");
                } catch (Exception e) {
                    System.err.println("Error in conditional processing: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
            worker.start();
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private boolean processSegmentOnNode(String address, int port, String segmentId, String operationType) {
        try (Socket socket = new Socket(address, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            
            out.writeObject("PROCESS_SEGMENT");
            out.writeObject(segmentId);
            out.writeObject(operationType);
            
            String response = (String) in.readObject();
            return "PROCESSED".equals(response);
            
        } catch (Exception e) {
            return false;
        }
    }
    
    @Override
    public Integer[] collectResults() {
        Integer[] results = new Integer[data.length];
        int segmentSize = data.length / nodeAddresses.length;
        
        for (int i = 0; i < nodeAddresses.length; i++) {
            String segmentId = "segment_" + i;
            Integer[] segment = getSegmentFromNode(nodeAddresses[i], ports[i], segmentId);
            
            if (segment != null) {
                int start = i * segmentSize;
                System.arraycopy(segment, 0, results, start, segment.length);
            }
        }
        
        return results;
    }
    
    private Integer[] getSegmentFromNode(String address, int port, String segmentId) {
        try (Socket socket = new Socket(address, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            
            out.writeObject("GET_SEGMENT");
            out.writeObject(segmentId);
            
            return (Integer[]) in.readObject();
            
        } catch (Exception e) {
            return null;
        }
    }
    
    @Override
    public void setReplicationFactor(int factor) {
        this.replicationFactor = factor;
    }
}