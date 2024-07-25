import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';

export class RestService {
  private client: AxiosInstance

  constructor(baseUrls: string[], isForward = false) {
    this.client = axios.create({
      baseURL: baseUrls[0], // Use the first base URL as the default
      timeout: 5000, // Default timeout
      headers: {},
    })

    if (isForward) {
      this.client.defaults.headers.common['X-Forward'] = 'true'
    }
  }

  public async sendHttpRequest<T>(
    url: string,
    method: 'GET' | 'POST' | 'PUT' | 'DELETE',
    data?: any,
    config?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {
    try {
      const response = await this.client.request<T>({
        url,
        method,
        data,
        ...config,
      })
      return response
    } catch (error) {
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(`HTTP error: ${error.response.status} - ${error.response.data}`)
      } else {
        const err = error as Error;
        throw new Error(`Unknown error: ${err.message}`)
      }
    }
  }

  public setHeaders(headers: Record<string, string>): void {
    this.client.defaults.headers.common = { ...this.client.defaults.headers.common, ...headers }
  }

  public setAuth(basicAuth?: string, bearerToken?: string): void {
    if (basicAuth) {
      this.client.defaults.headers.common['Authorization'] = `Basic ${basicAuth}`
    }

    if (bearerToken) {
      this.client.defaults.headers.common['Authorization'] = `Bearer ${bearerToken}`
    }
  }

  public setTimeout(timeout: number): void {
    this.client.defaults.timeout = timeout
  }

  public setBaseURL(baseUrl: string): void {
    this.client.defaults.baseURL = baseUrl
  }
}
