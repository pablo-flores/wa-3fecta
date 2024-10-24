db.alarm.aggregate([
  // Paso 1: Filtrar los documentos que tienen los alarmStates relevantes
  {
    $match: {
      alarmState: { $in: ['RAISED', 'UPDATED', 'RETRY', 'CLEARED'] },
      // Agrega otros filtros aquí si es necesario para reducir el conjunto de datos
    }
  },
  // Paso 2: Proyectar solo los campos necesarios
  {
    $project: {
      _id: 0,
      networkElementId: 1,
      alarmRaisedTime: 1,
      alarmState: 1,
      // Incluye otros campos que necesites en las etapas posteriores
      // Por ejemplo, si necesitas "alarmId" y otros campos, inclúyelos aquí
      alarmId: 1,
      // Otros campos...
    }
  },
  // Paso 3: Agrupar por networkElementId y alarmRaisedTime, y recopilar los alarmStates y los documentos completos
  {
    $group: {
      _id: {
        networkElementId: "$networkElementId",
        alarmRaisedTime: "$alarmRaisedTime"
      },
      alarmStates: { $addToSet: "$alarmState" },
      alarms: { $push: "$$ROOT" }
    }
  },
  // Paso 4: Filtrar los grupos que tienen al menos un 'CLEARED' y uno de los otros estados
  {
    $match: {
      $and: [
        { alarmStates: { $in: ['CLEARED'] } },
        { alarmStates: { $in: ['RAISED', 'UPDATED', 'RETRY'] } }
      ]
    }
  },
  // Paso 5: Descomponer el array de alarmas para obtener los documentos individuales
  {
    $unwind: "$alarms"
  },
  // Paso 6: Reemplazar la raíz del documento con cada alarma individual
  {
    $replaceRoot: { newRoot: "$alarms" }
  }
],
{
  allowDiskUse: true // Habilita el uso de disco
})
