import fs from "fs";
import { readdir, lstat } from "fs/promises";
import path, { resolve } from "path";
import readline from "readline";
import camelCase from "camelcase";
import os from "os";
import { parse } from "csv";
import pluralize from "pluralize";

const PATH = path.resolve("./data/csv");

type ModelMetadata = {
  column: string;
  type: string;
};

type Model = {
  name: string;
  columns: string[];
  metadata?: ModelMetadata;
};

const getModelMetadata = (model: Model) => {
  const metadata: ModelMetadata[] = [];
  model.columns.forEach((column) => {
    if (column === "id") {
      metadata.push({ column, type: "int" });
      return;
    }
    if (/^is[A-Z]/.test(column)) {
      metadata.push({ column, type: "boolean" });
      return;
    }
    if (/Id$/.test(column)) {
      metadata.push({ column, type: "int" });
      return;
    }
    metadata.push({ column, type: "string" });
  });

  return metadata;
};

const modelFormat = (model: Model) => {
  const columns = model.columns.map((column) => {
    if (column === "id") {
      return `${column} Int @id @default(autoincrement())`;
    }
    if (/^is[A-Z]/.test(column)) {
      return `${column} Boolean`;
    }
    if (/Id$/.test(column)) {
      return `${column} Int
  ${column.split("Id")[0]} ${pluralize(
        camelCase(column.split("Id")[0], {
          pascalCase: true,
        })
      )} @relation(fields: [${column}], references: [id], onDelete: Cascade)`;
    }
    return `${column} String`;
  });

  return `model ${model.name} {
  ${columns.join(`${os.EOL}  `)}
}

`;
};

const formatData = (data: any[], modelMetadata: ModelMetadata[]) => {
  const sample = data[0];
  if (!sample) {
    return data;
  }
  const keys = Object.keys(sample);
  const convertedData = data.map((d) => {
    const convertedD: Record<string, any> = {};
    for (const key of keys) {
      const modelMeta = modelMetadata.find((mm) => mm.column === key);
      if (!modelMeta) {
        continue;
      }
      if (modelMeta.type === "int") {
        convertedD[key] = parseInt(d[key]);
        continue;
      }
      if (modelMeta.type === "boolean") {
        convertedD[key] = Boolean(d[key]);
        continue;
      }
      convertedD[key] = d[key];
    }
    return convertedD;
  });
  return convertedData;
};

const getData = (filePath: string) => {
  return new Promise<any[]>((resolve, reject) => {
    const readStream = fs
      .createReadStream(filePath)
      .pipe(parse({ columns: true }));
    const data: any[] = [];
    readStream.on("data", (chunk) => {
      data.push(chunk);
    });
    readStream.on("end", () => {
      resolve(data);
    });
  });
};

const transformCsv = async () => {
  const data = await readdir(PATH);
  const fileNames: string[] = [];
  for (const fileName of data) {
    if (
      (await lstat(path.resolve(PATH, fileName))).isFile() &&
      !fileName.toLowerCase().includes("conquest")
    ) {
      fileNames.push(fileName);
    }
  }
  const modelData = fs.createWriteStream("./data/models.txt");
  for (const fileName of fileNames) {
    console.log(fileName);
    console.time("convertion-time");
    const data = await getData(path.resolve(PATH, fileName));
    const [sampleData] = data;
    const keys = sampleData ? Object.keys(sampleData) : [];
    const columns = keys.map((key) => camelCase(key));
    const model = camelCase(fileName.split(".")[0], { pascalCase: true });
    const wsData = fs.createWriteStream(
      path.resolve("./data/json", `${model}.json`)
    );
    const camelCasedData: any[] = [];
    for (const d of data) {
      const ccd: Record<string, string> = {};
      for (const key of keys) {
        ccd[camelCase(key)] = d[key];
      }
      camelCasedData.push(ccd);
    }
    const modelMetadata = getModelMetadata({ columns, name: model });
    const formatedData = formatData(camelCasedData, modelMetadata);
    wsData.write(JSON.stringify(formatedData));
    modelData.write(modelFormat({ name: model, columns }));
    console.timeEnd("convertion-time");
  }
};

transformCsv();
