import { Outlet, useParams } from 'react-router-dom'
import { IndexTabBar } from './IndexTabBar'

export function IndexLayout() {
  const { indexName } = useParams<{ indexName: string }>()

  if (!indexName) {
    return <Outlet />
  }

  return (
    <>
      <IndexTabBar indexName={indexName} />
      <Outlet />
    </>
  )
}
