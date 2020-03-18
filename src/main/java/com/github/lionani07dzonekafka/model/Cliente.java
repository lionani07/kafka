package com.github.lionani07dzonekafka.model;

import java.util.Date;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.format.annotation.DateTimeFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Cliente implements Serializer<Cliente> {	
	
	private Long id;
	private String name;
	private int age;
	
	@DateTimeFormat(pattern = "yyyy-MM-dd")
	private Date dateNascimento;
	
	public Cliente() {		
	}
	
	public Cliente(String name, int age, Date dateNascimento) {
		super();
		this.name = name;
		this.age = age;
		this.dateNascimento = dateNascimento;
	}

	public Long getId() {
		return id;
	}
	
	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public Date getDateNascimento() {
		return dateNascimento;
	}

	public void setDateNascimento(Date dateNascimento) {
		this.dateNascimento = dateNascimento;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Cliente other = (Cliente) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public byte[] serialize(String topic, Cliente data) {
		try {
			return new ObjectMapper().writeValueAsString(data).getBytes();
		} catch (JsonProcessingException e) {					
			throw new RuntimeException("Error serializando obj");
		}
		
	}
	
	
	
	

}
