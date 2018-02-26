package br.com.hadoop.leo.usuariosporregiao.job.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Classe Reducer
 * @author quartieri
 *
 */
public class UsuariosPorRegiaoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	

	/**
	 * Metodo herdado de Reducer, realiza a operação de reducao... e salva na saida de dados
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	
		   // Standard algorithm for finding the max value
        int maxMagnitude = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxMagnitude = Math.max(maxMagnitude, value.get());
        }
        
        context.write(key, new IntWritable(maxMagnitude));

	}	
}