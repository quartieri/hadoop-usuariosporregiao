package br.com.hadoop.leo.usuariosporregiao.job.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.com.hadoop.leo.usuariosporregiao.constantes.Constantes;

/**
 * 
 * Classe Mapper
 * @author quartieri
 *
 */
public class UsuariosPorRegiaoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int INDICE_NOME_REGIAO = 9;

	/**
	 *  Quebra a linha em um 'array de string' e adiciona no mapa 'contexto'
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] line = value.toString().split(Constantes.DELIMITADOR_VIRGULA, Constantes.COMPRIMENTO_DA_LINHA);

		// Ignora linhas mal formatadas
		if (line.length != Constantes.COMPRIMENTO_DA_LINHA) {
			System.out.println("- " + line.length);
			return;
		}

		// Pega a coluna com o dado 'region_name', para ser a chave 
		String outputKey = line[INDICE_NOME_REGIAO];


		// Record the output in the Context object
		context.write(new Text(outputKey), Constantes.UM_WRITABLE);

	}
}
