package es.unex.cum.sinf.practica3.pab_her.Consulta1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The type Consulta 1 mapper.
 */
public class Consulta1Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        Double temperatura = null;
        Double humedad = null;
        Double presion_atm = null;
        Double radiacion_solar = null;

        try {
            temperatura = Double.parseDouble(format(values[8])) < -9900d ? null : Double.parseDouble(format(values[8]));
        } catch (NumberFormatException ignored) {}
        try {
            humedad = Double.parseDouble(format(values[16])) < -9900d ? null : Double.parseDouble(format(values[16]));
        } catch (NumberFormatException ignored) {}
        try {
            presion_atm = Double.parseDouble(format(values[4])) < -9900d ? null : Double.parseDouble(format(values[4]));
        } catch (NumberFormatException ignored) {}
        try {
            radiacion_solar = Double.parseDouble(format(values[7])) < -9900d ? null : Double.parseDouble(format(values[7]));
        } catch (NumberFormatException ignored) {}

        final String region = format(values[20]);

        if (!region.equals("") && (temperatura != null || humedad != null || presion_atm != null || radiacion_solar != null)) {
            context.write(new Text(region), new Text(String.format("%f;%f;%f;%f", temperatura, humedad, presion_atm, radiacion_solar)));
        }
    }

    private String format(String value) {
        return value.trim();
    }
}
