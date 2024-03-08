package rinha.db;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RinhaDB {
    static <T extends Serializable> byte[] serialize(T obj) throws IOException {
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        ObjectOutputStream objectOutput = new ObjectOutputStream(byteArrayOutput);
        objectOutput.writeObject(obj);
        objectOutput.close();
        return byteArrayOutput.toByteArray();
    }

    static <T extends Serializable> int[] unsignedSerialize(T obj) throws IOException {
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        ObjectOutputStream objectOutput = new ObjectOutputStream(byteArrayOutput);
        objectOutput.writeObject(obj);
        objectOutput.close();
        final byte[] sBytes = byteArrayOutput.toByteArray();
        int[] uBytes = new int[sBytes.length];
        for (int i = 0; i < sBytes.length; i++) {
            uBytes[i] = ((int) sBytes[i]) & 0xff;
        }
        return uBytes;
    }

    public static <T extends Serializable> T deserialize(byte[] b, Class<T> cl) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object o = ois.readObject();
        return cl.cast(o);
    }

    public static <T extends Serializable> T deserialize(int[] in, Class<T> cl) throws IOException, ClassNotFoundException {
        byte[] b = new byte[in.length];
        for (int i = 0; i < in.length; i++) {
            b[i] = (byte) in[i];
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object o = ois.readObject();
        return cl.cast(o);
    }

    static byte[] convertLongToByteArray(long value) {
        byte[] bytes = new byte[Long.BYTES];
        int length = bytes.length;
        for (int i = 0; i < length; i++) {
            bytes[length - i - 1] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        var start = System.currentTimeMillis();
        var page = new Page();
        page.insert("rinha");
        page.insert("de");

        System.err.format("One insert %,d ms\n", System.currentTimeMillis() - start);

        var start2 = System.currentTimeMillis();
        List<byte[]> rows = page.rows();

        for (byte[] row : rows) {
            System.out.println(deserialize(row, String.class));
        }

        System.err.format("One insert %,d ms\n", System.currentTimeMillis() - start2);
    }


    private static class Page {
        private static final int PAGE_SIZE = 4096;
        private static final int ROW_SIZE = 256;

        ByteArrayOutputStream data;

        public Page() {
            this.data = new ByteArrayOutputStream(PAGE_SIZE);
        }

        public <T extends Serializable> void insert(T row) throws IOException {
            var serialized = unsignedSerialize(row); // transformando em array de bytes
            final int size = serialized.length; // tamanho do dado
            final byte[] sizeBytes = convertLongToByteArray(size); // transformando o tamanho em bytes, array de bytes
            final int sizeComplemented = ROW_SIZE - (size + sizeBytes.length);

            var aux = new byte[sizeComplemented];

            data.write(sizeBytes);

            for (int j : serialized) {
                data.write(j);
            }

            data.write(aux);
            data.close();
        }

        public List<byte[]> rows() {
            byte[] bytes = data.toByteArray();
            int s = bytes.length;
            int cursor = 0;
            int j = 0;

            Iterator<byte[]> rows = new Iterator<byte[]>(PAGE_SIZE) {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public byte[] next() {
                    return new byte[0];
                }
            };

            while (cursor < s) {
                var offset = cursor * ROW_SIZE;
                if (offset + ROW_SIZE > s) return rows;

                var row = Arrays.copyOfRange(bytes, offset, offset + ROW_SIZE);
                var size = Arrays.copyOfRange(row, 0, 8);
                var l = size.length;
                var mySizeValue = size[l - 1];
                var myRow = Arrays.copyOfRange(row, 8, 8 + mySizeValue);

                rows.add(myRow);

                cursor += 1;
            }

            return rows;
        }

//        public static boolean isSerializable(Class<?> it) {
//            boolean serializable = it.isPrimitive() || it.isInterface() || Serializable.class.isAssignableFrom(it);
//            if (!serializable) {
//                return false;
//            }
//            Field[] declaredFields = it.getDeclaredFields();
//            for (Field field : declaredFields) {
//                if (Modifier.isVolatile(field.getModifiers()) || Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
//                    continue;
//                }
//                Class<?> fieldType = field.getType();
//                if (!isSerializable(fieldType)) {
//                    return false;
//                }
//            }
//            return true;
//        }
    }
}












/*
 * linhas de tamanho fixo 512 bytes
 * cada pag tem um tamanho fixo de 4k bytes
 *
 * */