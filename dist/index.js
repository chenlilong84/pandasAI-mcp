#!/usr/bin/env node

/**
 * PandasAI MCP Service - Node.js版本
 * 基于Express的数据分析MCP服务
 */

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const XLSX = require('xlsx');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// 导入模块
const MCPHandler = require('./src/mcp-handler');
const DataAnalyzer = require('./src/data-analyzer');
const LLMManager = require('./src/llm-manager');

// 初始化Express应用
const app = express();
const port = process.env.PORT || 8001;
const host = process.env.HOST || '0.0.0.0';

// 中间件配置
app.use(cors({
  origin: '*',
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['*']
}));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// 文件上传配置
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const uniqueName = `${uuidv4()}-${file.originalname}`;
    cb(null, uniqueName);
  }
});

const upload = multer({ 
  storage,
  limits: {
    fileSize: 100 * 1024 * 1024 // 100MB
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ['.csv', '.xlsx', '.xls'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowedTypes.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error('不支持的文件格式。请上传CSV或Excel文件。'));
    }
  }
});

// 初始化服务组件
const mcpHandler = new MCPHandler();
const dataAnalyzer = new DataAnalyzer();
const llmManager = new LLMManager();

// 全局状态
let currentDataframe = null;
let currentLLM = null;

// ==================== MCP协议路由 ====================

/**
 * MCP协议主处理端点
 */
app.post('/mcp', async (req, res) => {
  try {
    const { method, params = {} } = req.body;
    
    console.log(`📨 MCP请求: ${method}`, params);
    
    let result;
    
    switch (method) {
      case 'initialize':
        result = await mcpHandler.initialize(params);
        break;
        
      case 'notifications/initialized':
        result = { success: true };
        break;
        
      case 'tools/list':
        result = await mcpHandler.listTools();
        break;
        
      case 'tools/call':
        result = await handleToolCall(params);
        break;
        
      default:
        throw new Error(`不支持的MCP方法: ${method}`);
    }
    
    res.json({ result });
    
  } catch (error) {
    console.error('❌ MCP处理错误:', error);
    res.status(400).json({
      error: {
        code: -1,
        message: error.message
      }
    });
  }
});

/**
 * 处理工具调用
 */
async function handleToolCall(params) {
  const { name, arguments: args } = params;
  
  switch (name) {
    case 'analyze_data':
      return await handleAnalyzeData(args);
      
    case 'configure_llm':
      return await handleConfigureLLM(args);
      
    default:
      throw new Error(`未知的工具: ${name}`);
  }
}

/**
 * 处理数据分析工具调用
 */
async function handleAnalyzeData(args) {
  try {
    const { query, llm_config } = args;
    
    if (!currentDataframe) {
      throw new Error('请先上传数据文件');
    }
    
    // 配置LLM（如果提供）
    if (llm_config) {
      currentLLM = await llmManager.configureLLM(llm_config);
    }
    
    if (!currentLLM) {
      throw new Error('请先配置LLM');
    }
    
    // 执行数据分析
    const result = await dataAnalyzer.analyze(currentDataframe, query, currentLLM);
    
    return {
      content: [{
        type: 'text',
        text: result.answer || '分析完成'
      }]
    };
    
  } catch (error) {
    throw new Error(`数据分析失败: ${error.message}`);
  }
}

/**
 * 处理LLM配置工具调用
 */
async function handleConfigureLLM(args) {
  try {
    currentLLM = await llmManager.configureLLM(args);
    
    return {
      content: [{
        type: 'text',
        text: `LLM配置成功: ${args.model}`
      }]
    };
    
  } catch (error) {
    throw new Error(`LLM配置失败: ${error.message}`);
  }
}

// ==================== SSE端点 ====================

/**
 * Server-Sent Events端点 - 用于实时数据流
 */
app.get('/sse', (req, res) => {
  // 设置SSE响应头
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Cache-Control'
  });

  // 发送初始连接消息
  res.write(`data: ${JSON.stringify({
    type: 'connection',
    message: '已连接到PandasAI MCP服务',
    timestamp: new Date().toISOString()
  })}\n\n`);

  // 定期发送心跳
  const heartbeat = setInterval(() => {
    res.write(`data: ${JSON.stringify({
      type: 'heartbeat',
      timestamp: new Date().toISOString()
    })}\n\n`);
  }, 30000); // 每30秒发送一次心跳

  // 发送服务状态
  const sendStatus = () => {
    res.write(`data: ${JSON.stringify({
      type: 'status',
      data: {
        service: 'PandasAI MCP Service',
        version: '1.0.0',
        status: 'running',
        dataLoaded: currentDataframe !== null,
        llmConfigured: currentLLM !== null,
        timestamp: new Date().toISOString()
      }
    })}\n\n`);
  };

  // 立即发送一次状态
  sendStatus();

  // 每分钟发送一次状态更新
  const statusInterval = setInterval(sendStatus, 60000);

  // 客户端断开连接时清理
  req.on('close', () => {
    clearInterval(heartbeat);
    clearInterval(statusInterval);
    console.log('📡 SSE客户端断开连接');
  });

  console.log('📡 新的SSE客户端连接');
});

// ==================== REST API路由 ====================

/**
 * 文件上传端点
 */
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: '请选择要上传的文件' });
    }
    
    const filePath = req.file.path;
    const ext = path.extname(req.file.originalname).toLowerCase();
    
    console.log(`📁 处理文件: ${req.file.originalname}`);
    
    // 读取文件数据
    let data;
    if (ext === '.csv') {
      data = await readCSVFile(filePath);
    } else if (ext === '.xlsx' || ext === '.xls') {
      data = await readExcelFile(filePath);
    } else {
      throw new Error('不支持的文件格式');
    }
    
    // 存储数据
    currentDataframe = {
      data,
      filename: req.file.originalname,
      rows: data.length,
      columns: data.length > 0 ? Object.keys(data[0]).length : 0
    };
    
    // 清理临时文件
    fs.unlinkSync(filePath);
    
    res.json({
      message: '文件上传成功',
      filename: req.file.originalname,
      rows: currentDataframe.rows,
      columns: currentDataframe.columns,
      preview: data.slice(0, 5) // 返回前5行预览
    });
    
  } catch (error) {
    console.error('❌ 文件上传错误:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * 数据分析端点
 */
app.post('/analyze', async (req, res) => {
  try {
    const { query, llm_config } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: '请提供分析查询' });
    }
    
    const result = await handleAnalyzeData({ query, llm_config });
    res.json(result);
    
  } catch (error) {
    console.error('❌ 数据分析错误:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * LLM配置端点
 */
app.post('/configure-llm', async (req, res) => {
  try {
    const result = await handleConfigureLLM(req.body);
    res.json(result);
    
  } catch (error) {
    console.error('❌ LLM配置错误:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * 状态检查端点
 */
app.get('/status', (req, res) => {
  res.json({
    status: 'running',
    service: 'PandasAI MCP Service (Node.js)',
    version: '1.0.0',
    timestamp: new Date().toISOString(),
    data_loaded: !!currentDataframe,
    llm_configured: !!currentLLM,
    dataframe_info: currentDataframe ? {
      filename: currentDataframe.filename,
      rows: currentDataframe.rows,
      columns: currentDataframe.columns
    } : null
  });
});

/**
 * API文档端点
 */
app.get('/docs', (req, res) => {
  res.json({
    title: 'PandasAI MCP Service API',
    version: '1.0.0',
    description: '基于Node.js的数据分析MCP服务',
    endpoints: {
      'POST /mcp': 'MCP协议主端点',
      'GET /sse': 'Server-Sent Events实时数据流',
      'POST /upload': '文件上传',
      'POST /analyze': '数据分析',
      'POST /configure-llm': 'LLM配置',
      'GET /status': '服务状态',
      'GET /docs': 'API文档'
    },
    sse: {
      endpoint: '/sse',
      description: 'Server-Sent Events端点，提供实时服务状态和心跳',
      events: {
        connection: '连接建立事件',
        heartbeat: '心跳事件（每30秒）',
        status: '服务状态事件（每分钟）'
      }
    }
  });
});

/**
 * 根路径
 */
app.get('/', (req, res) => {
  res.json({
    message: '🚀 PandasAI MCP Service (Node.js版本)',
    version: '1.0.0',
    docs: '/docs',
    status: '/status'
  });
});

// ==================== 工具函数 ====================

/**
 * 读取CSV文件
 */
function readCSVFile(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', reject);
  });
}

/**
 * 读取Excel文件
 */
function readExcelFile(filePath) {
  try {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);
    return Promise.resolve(data);
  } catch (error) {
    return Promise.reject(error);
  }
}

// ==================== 错误处理 ====================

// 全局错误处理
app.use((error, req, res, next) => {
  console.error('❌ 服务器错误:', error);
  res.status(500).json({
    error: '内部服务器错误',
    message: error.message
  });
});

// 404处理
app.use((req, res) => {
  res.status(404).json({
    error: '端点未找到',
    path: req.path
  });
});

// ==================== 服务启动 ====================

if (require.main === module) {
  app.listen(port, host, () => {
    console.log('\n🚀 PandasAI MCP Service (Node.js) 启动成功!');
    console.log(`📍 服务地址: http://${host}:${port}`);
    console.log(`📚 API文档: http://${host}:${port}/docs`);
    console.log(`📊 状态检查: http://${host}:${port}/status`);
    console.log('\n💡 使用npx启动: npx pandasai-mcp start');
    console.log('\n按 Ctrl+C 停止服务\n');
  });
}

module.exports = app;